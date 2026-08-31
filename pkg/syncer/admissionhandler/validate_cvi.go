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
	"fmt"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// cviServiceFactory matches csivolumeinfosvc.InitCsiVolumeInfoService's signature.
type cviServiceFactory func(ctx context.Context) (csivolumeinfosvc.CsiVolumeInfoService, error)

// newCviService is a package-level function variable defaulting to the real
// constructor. Tests can override it to inject a fake CsiVolumeInfoService
// without relying on architecture-specific monkey patching, mirroring
// newK8sClient's pattern in validatepvc.go.
var newCviService cviServiceFactory = csivolumeinfosvc.InitCsiVolumeInfoService

const (
	// DeleteVMOwnedVolumeErrorMessageFormat is the rejection message returned when
	// a PVC delete is attempted while the volume is VM-managed. It takes the PVC
	// name as its single argument.
	DeleteVMOwnedVolumeErrorMessageFormat = "Cannot delete PVC %s: volume is VM-managed. " +
		"Detach the volume from the VM or delete all retaining snapshots first."

	// SnapshotVMOwnedVolumeErrorMessageFormat is the rejection message returned when
	// a VolumeSnapshot create is attempted while the volume is VM-managed. It takes
	// the PVC name as its single argument.
	SnapshotVMOwnedVolumeErrorMessageFormat = "Cannot create snapshot for PVC %s: volume is VM-managed. " +
		"Detach the volume from the VM or delete all retaining snapshots first."

	// MixedDiskModeErrorMessageFormat is the rejection message returned when a
	// CsiVolumeInfo's spec.vms mixes a Persistent (dependent) entry with an
	// Independent* entry. It takes the CsiVolumeInfo name as its single argument.
	MixedDiskModeErrorMessageFormat = "CsiVolumeInfo %s: spec.vms mixes a Persistent entry with an " +
		"Independent* entry; a volume has one physical form and must attach in a single disk mode " +
		"across all VMs"
)

// validatePVCDeletionForVMOwnedVolumes checks whether a PVC being deleted has
// a corresponding CsiVolumeInfo that is either VMManaged or still has an
// attached VM in spec.vms. If so, the deletion is rejected. The second
// disjunct is what covers an attached **independent** volume, which stays
// CSIManaged for the life of the attachment — keying on ownership alone
// would leave it unprotected.
//
// This guard is a no-op when the VMOwnedVolumes feature state is disabled,
// when the CsiVolumeInfo cannot be resolved (fail-open to avoid blocking
// unrelated deletes), or when the volume is CSI-managed with no VM attached.
func validatePVCDeletionForVMOwnedVolumes(
	ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	if !featureGateVMOwnedVolumesEnabled {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if req.Operation != admissionv1.Delete {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Decode the PVC being deleted.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(req.OldObject.Raw, pvc); err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to decode PVC from request: %v; "+
			"allowing deletion (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Only examine bound PVCs that have a named PV.
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	k8sclient, err := newK8sClient(ctx)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to create k8s client: %v; "+
			"allowing deletion (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Resolve PV → volumeHandle (O(1) Get by name).
	pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to get PV %q for PVC %s/%s: %v; "+
			"allowing deletion (fail-open)", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Resolve CsiVolumeInfo by deterministic name (O(1) Get).
	cviSvc, err := newCviService(ctx)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to init CVI service for PVC %s/%s: %v; "+
			"allowing deletion (fail-open)", pvc.Namespace, pvc.Name, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		log.Warnf("validatePVCDeletionForVMOwnedVolumes: failed to get CsiVolumeInfo for volume %q: %v; "+
			"allowing deletion (fail-open)", volumeID, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if cvi == nil {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	vmManaged := cvi.Status.Ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged
	attached := len(cvi.Spec.VMs) > 0
	if !vmManaged && !attached {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// The VM(s) that own/attach this volume may already be gone — most commonly
	// because their namespace was deleted out from under them. A deleted VM can
	// never come back to release the volume, so continuing to block the PVC
	// delete would only leave it stuck forever and get in the way of namespace
	// cleanup. Allow the delete once we can confirm there's no VM left to protect.
	if attached {
		if !anyReferencedVMExists(ctx, pvc.Namespace, cvi.Spec.VMs) {
			log.Infof("validatePVCDeletionForVMOwnedVolumes: allowing delete of PVC %s/%s (volumeID=%q); "+
				"none of the referenced VMs %v exist any more", pvc.Namespace, pvc.Name, volumeID,
				vmNames(cvi.Spec.VMs))
			return &admissionv1.AdmissionResponse{Allowed: true}
		}
	} else if isNamespaceBeingDeleted(ctx, pvc.Namespace) {
		// VMManaged with no attached VM on record: there's no specific VM to
		// check, so fall back to the namespace's own deletion state.
		log.Infof("validatePVCDeletionForVMOwnedVolumes: allowing delete of PVC %s/%s (volumeID=%q); "+
			"volume is VMManaged with no VM on record and namespace %s is being deleted",
			pvc.Namespace, pvc.Name, volumeID, pvc.Namespace)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Volume is VM-managed, or still has an attached (independent) VM — reject the deletion.
	log.Infof("validatePVCDeletionForVMOwnedVolumes: rejecting delete of PVC %s/%s "+
		"(volumeID=%q, ownership=%q, attachedVMs=%d)", pvc.Namespace, pvc.Name, volumeID,
		cvi.Status.Ownership, len(cvi.Spec.VMs))
	msg := fmt.Sprintf(DeleteVMOwnedVolumeErrorMessageFormat, pvc.Name)
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: msg,
			Reason:  metav1.StatusReasonForbidden,
			Code:    403,
		},
	}
}

// vmExistsFunc matches checkVMExists's signature. Tests can override the
// checkVMExists package var to inject a fake without a real vm-operator client.
type vmExistsFunc func(ctx context.Context, namespace, vmName string) (bool, error)

var checkVMExists vmExistsFunc = defaultCheckVMExists

// defaultCheckVMExists reports whether the named VirtualMachine still exists.
func defaultCheckVMExists(ctx context.Context, namespace, vmName string) (bool, error) {
	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return false, err
	}
	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatortypes.GroupName)
	if err != nil {
		return false, err
	}
	_, _, err = getVirtualMachine(ctx, vmOperatorClient, vmName, namespace)
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// anyReferencedVMExists reports whether at least one VM named in vms still
// exists in namespace. It fails open (returns true, i.e. assume the VM is
// still there) on any error checking a VM, so a transient lookup failure
// never causes a PVC delete to be wrongly allowed.
func anyReferencedVMExists(ctx context.Context, namespace string, vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
	log := logger.GetLogger(ctx)

	for _, vmRef := range vms {
		exists, err := checkVMExists(ctx, namespace, vmRef.VMName)
		if err != nil {
			log.Warnf("anyReferencedVMExists: failed to check VM %s/%s: %v; assuming it still exists",
				namespace, vmRef.VMName, err)
			return true
		}
		if exists {
			return true
		}
	}
	return false
}

// vmNames returns the VMName of each entry in vms, for logging.
func vmNames(vms []csivolumeinfov1alpha1.VirtualMachineRef) []string {
	names := make([]string, len(vms))
	for i, vm := range vms {
		names[i] = vm.VMName
	}
	return names
}

// validateSnapshotCreateForVMOwnedVolumes rejects VolumeSnapshot CREATE requests
// when the source PVC's volume has ownership=VMManaged. The disk is a plain VMDK
// at that point — no FCD entry exists in CNS — so an FCD snapshot would fail or
// produce an inconsistent result.
//
// Fail-open on resolution errors to avoid blocking unrelated snapshot creates.
func validateSnapshotCreateForVMOwnedVolumes(
	ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	if !featureGateVMOwnedVolumesEnabled {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if req.Operation != admissionv1.Create {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	vs := &snap.VolumeSnapshot{}
	if err := json.Unmarshal(req.Object.Raw, vs); err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to decode VolumeSnapshot: %v; "+
			"allowing (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	// Only examine snapshots that name an explicit PVC source.
	if vs.Spec.Source.PersistentVolumeClaimName == nil || *vs.Spec.Source.PersistentVolumeClaimName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	pvcName := *vs.Spec.Source.PersistentVolumeClaimName
	pvcNamespace := req.Namespace

	k8sclient, err := newK8sClient(ctx)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to create k8s client: %v; "+
			"allowing (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get PVC %s/%s: %v; "+
			"allowing (fail-open)", pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	pv, err := k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get PV %q for PVC %s/%s: %v; "+
			"allowing (fail-open)", pvc.Spec.VolumeName, pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cviSvc, err := newCviService(ctx)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to init CVI service for PVC %s/%s: %v; "+
			"allowing (fail-open)", pvcNamespace, pvcName, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		log.Warnf("validateSnapshotCreateForVMOwnedVolumes: failed to get CsiVolumeInfo for volume %q: %v; "+
			"allowing (fail-open)", volumeID, err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if cvi == nil || cvi.Status.Ownership != csivolumeinfov1alpha1.OwnershipStateVMManaged {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	log.Infof("validateSnapshotCreateForVMOwnedVolumes: rejecting VolumeSnapshot %s/%s for PVC %s/%s "+
		"(volumeID=%q, ownership=VMManaged)", req.Namespace, vs.Name, pvcNamespace, pvcName, volumeID)
	msg := fmt.Sprintf(SnapshotVMOwnedVolumeErrorMessageFormat, pvcName)
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: msg,
			Reason:  metav1.StatusReasonForbidden,
			Code:    403,
		},
	}
}

// hasMixedDiskModes reports whether vms contains both a Persistent
// (dependent — including the empty-defaults-to-Persistent case) entry and an
// Independent* entry. A volume has one physical form, so mixing the two
// classes across VMs attaching the same volume is unsupported.
func hasMixedDiskModes(vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
	hasDependent, hasIndependent := false, false
	for _, vm := range vms {
		if vm.DiskMode == "" || vm.DiskMode == csivolumeinfov1alpha1.DiskModePersistent {
			hasDependent = true
		} else {
			hasIndependent = true
		}
	}
	return hasDependent && hasIndependent
}

// validateCsiVolumeInfoSingleMode rejects a CsiVolumeInfo CREATE/UPDATE whose
// spec.vms mixes a Persistent entry with an Independent* entry. This
// intercepts vm-operator's own writes to the CVI, so fails open on a decode
// error exactly as every other handler in this file does — a bug here must
// not block all attach progress.
func validateCsiVolumeInfoSingleMode(
	ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logger.GetLogger(ctx)

	if !featureGateVMOwnedVolumesEnabled {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	if req.Operation != admissionv1.Create && req.Operation != admissionv1.Update {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	if err := json.Unmarshal(req.Object.Raw, cvi); err != nil {
		log.Warnf("validateCsiVolumeInfoSingleMode: failed to decode CsiVolumeInfo: %v; allowing (fail-open)", err)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	if !hasMixedDiskModes(cvi.Spec.VMs) {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	log.Infof("validateCsiVolumeInfoSingleMode: rejecting CsiVolumeInfo %s: spec.vms mixes dependent and "+
		"independent entries", cvi.Name)
	msg := fmt.Sprintf(MixedDiskModeErrorMessageFormat, cvi.Name)
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Message: msg,
			Reason:  metav1.StatusReasonForbidden,
			Code:    403,
		},
	}
}
