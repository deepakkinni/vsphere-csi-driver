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

package admissionhandler

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"k8s.io/apimachinery/pkg/runtime"
	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

func withFeatureGateVMOwnedVolumes(t *testing.T, enabled bool) {
	t.Helper()
	orig := featureGateVMOwnedVolumesEnabled
	featureGateVMOwnedVolumesEnabled = enabled
	t.Cleanup(func() { featureGateVMOwnedVolumesEnabled = orig })
}

func withCviService(t *testing.T, cvis ...*csivolumeinfov1alpha1.CsiVolumeInfo) {
	t.Helper()
	s := runtime.NewScheme()
	if err := csivolumeinfov1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	objs := make([]client.Object, len(cvis))
	for i, c := range cvis {
		objs[i] = c
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		Build()
	svc := csivolumeinfosvc.NewCsiVolumeInfoService(c)

	orig := newCviService
	newCviService = func(ctx context.Context) (csivolumeinfosvc.CsiVolumeInfoService, error) {
		return svc, nil
	}
	t.Cleanup(func() { newCviService = orig })
}

func withK8sClient(t *testing.T, objs ...client.Object) {
	t.Helper()
	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		if ro, ok := o.(runtime.Object); ok {
			runtimeObjs = append(runtimeObjs, ro)
		}
	}
	fakeK8sClient := fakeclientset.NewSimpleClientset(runtimeObjs...)

	orig := newK8sClient
	newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
		return fakeK8sClient, nil
	}
	t.Cleanup(func() { newK8sClient = orig })
}

func buildBoundPVCAndPV(pvcName, pvcNamespace, pvName, volumeID string) (
	*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: pvcNamespace},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: csitypes.Name, VolumeHandle: volumeID},
			},
		},
	}
	return pvc, pv
}

func buildTestCVIWithVMs(volumeID string, ownership csivolumeinfov1alpha1.OwnershipState,
	vms []csivolumeinfov1alpha1.VirtualMachineRef) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfov1alpha1.CVINamePrefix + volumeID,
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{VolumeID: volumeID, VMs: vms},
	}
	cvi.Status.Ownership = ownership
	return cvi
}

// TestValidatePVCDeletionForVMOwnedVolumes_IndependentAttached_Rejects verifies
// the §13.2.1 widening: a PVC bound to an independent (CSIManaged-for-life)
// attachment is rejected on delete via the spec.vms disjunct, not ownership.
func TestValidatePVCDeletionForVMOwnedVolumes_IndependentAttached_Rejects(t *testing.T) {
	withFeatureGateVMOwnedVolumes(t, true)
	const volumeID = "vol-independent"
	pvc, pv := buildBoundPVCAndPV("test-pvc", "test-ns", "test-pv", volumeID)
	withK8sClient(t, pv)
	cvi := buildTestCVIWithVMs(volumeID, csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		[]csivolumeinfov1alpha1.VirtualMachineRef{
			{VMName: "vm-a", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
		})
	withCviService(t, cvi)

	pvcRaw, err := json.Marshal(pvc)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		OldObject: runtime.RawExtension{Raw: pvcRaw},
	}
	resp := validatePVCDeletionForVMOwnedVolumes(context.Background(), req)
	if resp.Allowed {
		t.Fatal("expected an attached independent PVC delete to be rejected")
	}
}

// TestValidatePVCDeletionForVMOwnedVolumes_NoVMsAttached_Allows verifies that
// a CSIManaged CVI with an empty spec.vms does not block PVC deletion.
func TestValidatePVCDeletionForVMOwnedVolumes_NoVMsAttached_Allows(t *testing.T) {
	withFeatureGateVMOwnedVolumes(t, true)
	const volumeID = "vol-idle"
	pvc, pv := buildBoundPVCAndPV("test-pvc", "test-ns", "test-pv", volumeID)
	withK8sClient(t, pv)
	cvi := buildTestCVIWithVMs(volumeID, csivolumeinfov1alpha1.OwnershipStateCSIManaged, nil)
	withCviService(t, cvi)

	pvcRaw, err := json.Marshal(pvc)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		OldObject: runtime.RawExtension{Raw: pvcRaw},
	}
	resp := validatePVCDeletionForVMOwnedVolumes(context.Background(), req)
	if !resp.Allowed {
		t.Fatal("expected an idle CSIManaged PVC delete to be allowed")
	}
}

// TestValidateSnapshotCreateForVMOwnedVolumes_IndependentOnly_NotBlocked is
// the regression test §13.2 calls for explicitly: the natural next edit to
// this file is to widen the snapshot-create guard for symmetry with the PVC
// delete widening above, and that must not happen — independent volumes stay
// snapshottable while attached.
func TestValidateSnapshotCreateForVMOwnedVolumes_IndependentOnly_NotBlocked(t *testing.T) {
	withFeatureGateVMOwnedVolumes(t, true)
	const volumeID = "vol-independent-snap"
	pvc, pv := buildBoundPVCAndPV("test-pvc", "test-ns", "test-pv", volumeID)
	withK8sClient(t, pvc, pv)
	cvi := buildTestCVIWithVMs(volumeID, csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		[]csivolumeinfov1alpha1.VirtualMachineRef{
			{VMName: "vm-a", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
		})
	withCviService(t, cvi)

	pvcName := "test-pvc"
	vs := map[string]interface{}{
		"metadata": map[string]interface{}{"name": "test-vs", "namespace": "test-ns"},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"persistentVolumeClaimName": pvcName},
		},
	}
	vsRaw, err := json.Marshal(vs)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: "test-ns",
		Object:    runtime.RawExtension{Raw: vsRaw},
	}
	resp := validateSnapshotCreateForVMOwnedVolumes(context.Background(), req)
	if !resp.Allowed {
		t.Fatal("expected snapshot create on an independent-only CVI to be allowed")
	}
}

// TestValidateCsiVolumeInfoSingleMode_Mixed_Rejects verifies the single-mode
// invariant webhook: a CsiVolumeInfo whose spec.vms mixes a Persistent entry
// with an Independent* entry is rejected.
func TestValidateCsiVolumeInfoSingleMode_Mixed_Rejects(t *testing.T) {
	withFeatureGateVMOwnedVolumes(t, true)
	cvi := buildTestCVIWithVMs("vol-mixed", "", []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-a", DiskMode: csivolumeinfov1alpha1.DiskModePersistent},
		{VMName: "vm-b", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
	})
	cviRaw, err := json.Marshal(cvi)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Object:    runtime.RawExtension{Raw: cviRaw},
	}
	resp := validateCsiVolumeInfoSingleMode(context.Background(), req)
	if resp.Allowed {
		t.Fatal("expected a mixed-mode CsiVolumeInfo to be rejected")
	}
}

// TestValidateCsiVolumeInfoSingleMode_SingleMode_Allows verifies that a
// uniform (all-dependent or all-independent) spec.vms is allowed.
func TestValidateCsiVolumeInfoSingleMode_SingleMode_Allows(t *testing.T) {
	withFeatureGateVMOwnedVolumes(t, true)
	cvi := buildTestCVIWithVMs("vol-uniform", "", []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-a", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
		{VMName: "vm-b", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentNonPersistent},
	})
	cviRaw, err := json.Marshal(cvi)
	if err != nil {
		t.Fatal(err)
	}
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Object:    runtime.RawExtension{Raw: cviRaw},
	}
	resp := validateCsiVolumeInfoSingleMode(context.Background(), req)
	if !resp.Allowed {
		t.Fatal("expected a uniform-mode CsiVolumeInfo to be allowed")
	}
}
