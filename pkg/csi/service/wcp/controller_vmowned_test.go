/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wcp

import (
	"context"
	"testing"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"k8s.io/apimachinery/pkg/runtime"
	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

func newCVIScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = csivolumeinfov1alpha1.AddToScheme(s)
	return s
}

func cviSvcWith(cvis ...*csivolumeinfov1alpha1.CsiVolumeInfo) csivolumeinfosvc.CsiVolumeInfoService {
	objs := make([]client.Object, len(cvis))
	for i, c := range cvis {
		objs[i] = c
	}
	c := fake.NewClientBuilder().
		WithScheme(newCVIScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		Build()
	return csivolumeinfosvc.NewCsiVolumeInfoService(c)
}

func buildTestCVI(volumeID string,
	ownership csivolumeinfov1alpha1.OwnershipState) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfov1alpha1.CVINamePrefix + volumeID,
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volumeID},
	}
	cvi.Status.Ownership = ownership
	return cvi
}

func TestAssertNotVMManaged_VMManaged_Rejects(t *testing.T) {
	svc := cviSvcWith(buildTestCVI("vol-001", csivolumeinfov1alpha1.OwnershipStateVMManaged))
	if err := assertNotVMManaged(context.Background(), svc, "vol-001", "snapshot creation", nil); err == nil {
		t.Fatal("expected error for VMManaged volume, got nil")
	}
}

func TestAssertNotVMManaged_CSIManaged_Allows(t *testing.T) {
	svc := cviSvcWith(buildTestCVI("vol-002", csivolumeinfov1alpha1.OwnershipStateCSIManaged))
	if err := assertNotVMManaged(context.Background(), svc, "vol-002", "snapshot creation", nil); err != nil {
		t.Fatalf("expected nil for CSIManaged volume, got: %v", err)
	}
}

func TestAssertNotVMManaged_NoCVI_Allows(t *testing.T) {
	svc := cviSvcWith() // empty
	if err := assertNotVMManaged(context.Background(), svc, "vol-003", "snapshot creation", nil); err != nil {
		t.Fatalf("expected nil (fail-open) when no CVI, got: %v", err)
	}
}

// TestAssertNotVMManaged_FcdRetained_Rejects verifies that a retained-FCD
// volume is refused for the same reason a plain VMDK is: the CVI, not CNS
// state, is authoritative — an fcd-retained volume still has a live FCD and
// CNS DB row, so a CNS-side check alone would answer normally instead of
// reporting the volume as absent.
func TestAssertNotVMManaged_FcdRetained_Rejects(t *testing.T) {
	cvi := buildTestCVI("vol-004", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	cvi.Annotations = map[string]string{csivolumeinfov1alpha1.FcdRetainedAnnotation: "true"}
	svc := cviSvcWith(cvi)
	if err := assertNotVMManaged(context.Background(), svc, "vol-004", "volume deletion", nil); err == nil {
		t.Fatal("expected error for fcd-retained volume, got nil")
	}
}

// TestAssertNotVMManaged_IndependentOnly_Allows verifies attach/detach §13.3's
// explicit carve-out: an independent attachment is CSIManaged for life and
// is not locked down, unlike a dependent (VMManaged) one.
func TestAssertNotVMManaged_IndependentOnly_Allows(t *testing.T) {
	cvi := buildTestCVI("vol-005", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	cvi.Spec.VMs = []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-a", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
	}
	svc := cviSvcWith(cvi)
	if err := assertNotVMManaged(context.Background(), svc, "vol-005", "snapshot creation", nil); err != nil {
		t.Fatalf("expected nil for an independent-only attachment, got: %v", err)
	}
}

// TestAssertNotVMManaged_VMGone_Allows verifies that a VMManaged volume can
// be deleted once vmGoneCheck confirms the owning VM no longer exists (e.g.
// its namespace was force-deleted): a gone VM can never come back to release
// the volume, so continuing to block would leave the PVC stuck forever.
func TestAssertNotVMManaged_VMGone_Allows(t *testing.T) {
	cvi := buildTestCVI("vol-006", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	cvi.Spec.VMs = []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-deleted"}}
	svc := cviSvcWith(cvi)
	vmGone := func(ctx context.Context, namespace string, vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
		return true
	}
	if err := assertNotVMManaged(context.Background(), svc, "vol-006", "volume deletion", vmGone); err != nil {
		t.Fatalf("expected nil once vmGoneCheck confirms the VM is gone, got: %v", err)
	}
}

// TestAssertNotVMManaged_VMStillPresent_Rejects verifies that a VMManaged
// volume stays blocked when vmGoneCheck reports the VM is still there.
func TestAssertNotVMManaged_VMStillPresent_Rejects(t *testing.T) {
	cvi := buildTestCVI("vol-007", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	cvi.Spec.VMs = []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	svc := cviSvcWith(cvi)
	vmGone := func(ctx context.Context, namespace string, vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
		return false
	}
	if err := assertNotVMManaged(context.Background(), svc, "vol-007", "volume deletion", vmGone); err == nil {
		t.Fatal("expected error while vmGoneCheck reports the VM still exists")
	}
}

// withVMOperatorClient stubs newVMOperatorClientForVMLookup to return a fake
// controller-runtime client seeded with vms, so vmOwnedVolumesVMsGone's
// VM-lookup path can be exercised without a real vm-operator client.
func withVMOperatorClient(t *testing.T, vms ...client.Object) {
	t.Helper()
	scheme := newCVIScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vms...).Build()

	orig := newVMOperatorClientForVMLookup
	newVMOperatorClientForVMLookup = func(ctx context.Context) (client.Client, error) {
		return fakeClient, nil
	}
	t.Cleanup(func() { newVMOperatorClientForVMLookup = orig })
}

// TestVMOwnedVolumesVMsGone_VMDeleted_ReturnsTrue verifies the VM-lookup path:
// none of the VMs named in vms exist in the fake vm-operator client, so the
// volume is considered safe to unblock.
func TestVMOwnedVolumesVMsGone_VMDeleted_ReturnsTrue(t *testing.T) {
	withVMOperatorClient(t) // no VMs exist
	c := &controller{}
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-deleted"}}
	if !c.vmOwnedVolumesVMsGone(context.Background(), "test-ns", vms) {
		t.Fatal("expected true once the referenced VM no longer exists")
	}
}

// TestVMOwnedVolumesVMsGone_VMStillExists_ReturnsFalse verifies that the
// volume stays protected while its VM is still present.
func TestVMOwnedVolumesVMsGone_VMStillExists_ReturnsFalse(t *testing.T) {
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: "vm-a", Namespace: "test-ns"},
	}
	withVMOperatorClient(t, vm)
	c := &controller{}
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a"}}
	if c.vmOwnedVolumesVMsGone(context.Background(), "test-ns", vms) {
		t.Fatal("expected false while the referenced VM still exists")
	}
}

// TestVMOwnedVolumesVMsGone_NoVMsNamespaceDeleted_ReturnsTrue verifies the
// namespace-fallback path used when a VMManaged CVI has no VM on record: a
// namespace absent from the lister is treated as already gone.
func TestVMOwnedVolumesVMsGone_NoVMsNamespaceDeleted_ReturnsTrue(t *testing.T) {
	k8sClient := fakeclientset.NewSimpleClientset() // namespace "test-ns" does not exist
	c := &controller{namespaceLister: testNamespaceLister(t, k8sClient)}
	if !c.vmOwnedVolumesVMsGone(context.Background(), "test-ns", nil) {
		t.Fatal("expected true when the namespace no longer exists")
	}
}

// TestVMOwnedVolumesVMsGone_NoVMsNamespacePresent_ReturnsFalse verifies that
// a live, non-terminating namespace keeps the volume protected when there's
// no specific VM to check.
func TestVMOwnedVolumesVMsGone_NoVMsNamespacePresent_ReturnsFalse(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-ns"}}
	k8sClient := fakeclientset.NewSimpleClientset(ns)
	c := &controller{namespaceLister: testNamespaceLister(t, k8sClient)}
	if c.vmOwnedVolumesVMsGone(context.Background(), "test-ns", nil) {
		t.Fatal("expected false while the namespace is still present and not terminating")
	}
}
