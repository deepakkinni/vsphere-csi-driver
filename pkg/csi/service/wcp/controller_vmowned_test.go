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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	if err := assertNotVMManaged(context.Background(), svc, "vol-001", "snapshot creation"); err == nil {
		t.Fatal("expected error for VMManaged volume, got nil")
	}
}

func TestAssertNotVMManaged_CSIManaged_Allows(t *testing.T) {
	svc := cviSvcWith(buildTestCVI("vol-002", csivolumeinfov1alpha1.OwnershipStateCSIManaged))
	if err := assertNotVMManaged(context.Background(), svc, "vol-002", "snapshot creation"); err != nil {
		t.Fatalf("expected nil for CSIManaged volume, got: %v", err)
	}
}

func TestAssertNotVMManaged_NoCVI_Allows(t *testing.T) {
	svc := cviSvcWith() // empty
	if err := assertNotVMManaged(context.Background(), svc, "vol-003", "snapshot creation"); err != nil {
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
	if err := assertNotVMManaged(context.Background(), svc, "vol-004", "volume deletion"); err == nil {
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
	if err := assertNotVMManaged(context.Background(), svc, "vol-005", "snapshot creation"); err != nil {
		t.Fatalf("expected nil for an independent-only attachment, got: %v", err)
	}
}
