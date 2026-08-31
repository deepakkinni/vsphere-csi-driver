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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// CRDSingular represents the singular name of CsiVolumeInfo CRD.
	CRDSingular = "csivolumeinfo"
	// CRDPlural represents the plural name of CsiVolumeInfo CRD.
	CRDPlural = "csivolumeinfos"

	// VolumeProtectionFinalizer prevents GC while ownership is VMManaged.
	VolumeProtectionFinalizer = "csi.vsphere.vmware.com/volume-protection"

	// PVCVolumeProtectionFinalizer is written by the CVI controller onto the
	// bound PVC while spec.vms is non-empty, and is the only thing preventing
	// deletion of an attached PVC for an independent volume: that CVI never
	// transitions to VMManaged, so VolumeProtectionFinalizer above never
	// applies to it. Deliberately a distinct key from the CnsNodeVMBatchAttachment
	// controller's own PVC finalizer — two controllers writing one finalizer
	// key cannot be reasoned about independently.
	PVCVolumeProtectionFinalizer = "csi.vsphere.vmware.com/pvc-volume-protection"

	// CVINamespace is the namespace where all CsiVolumeInfo CRs live.
	CVINamespace = "vmware-system-csi"

	// CVINamePrefix is prepended to the volumeID to form the CR name.
	CVINamePrefix = "cvi-volume-"

	// FcdRetainedAnnotation marks a VMManaged volume whose FCD was NOT
	// unregistered because an in-place unregister was blocked. The FCD, its CNS DB
	// row, and its FCD snapshots all still exist, so lock-down for such a volume
	// must be enforced by consulting this CR rather than by relying on CNS to
	// return NotFound.
	FcdRetainedAnnotation = "csi.vsphere.vmware.com/fcd-retained"

	// UnregisterAttemptedAnnotation records that reconcileUnregister has
	// reached its destructive UnregisterVolumeEx call for this volume. It is
	// written in the same metadata patch that adds VolumeProtectionFinalizer,
	// immediately before that call, and cleared by finishRelease once the
	// volume is back to CSIManaged.
	//
	// It exists because VolumeProtectionFinalizer cannot carry this meaning.
	// The finalizer answers "must GC be blocked?", which stops being true the
	// moment the last dependent VM detaches; this annotation answers "has the
	// FCD already been unregistered?", which stays true until the volume is
	// re-registered. Encoding both in the finalizer means any code acting on
	// the first fact destroys the evidence for the second — which is exactly
	// how a stray finalizer left by a cancelled mid-attach became
	// unremovable: removing it would have broken reconcileUnregister's
	// crash-recovery path, which keys on that same bit.
	//
	// Consumers must treat this as authoritative over status.ownership when
	// deciding whether a destructive call has already happened:
	// status.ownership is read through the informer cache and a racing
	// reconcile can observe a snapshot that predates an earlier reconcile's
	// own status write, whereas this annotation is written before the call it
	// guards.
	UnregisterAttemptedAnnotation = "csi.vsphere.vmware.com/unregister-attempted"

	// ImportPendingAnnotation marks a CsiVolumeInfo created by the
	// CnsRegisterVolume deferFcdRegistration ("import") path between the
	// moment the CR (with spec.vms already populated) is created and the
	// moment its status is patched to VMManaged. This volume has no backing
	// FCD by design (see the vm-owned-volume-vm-import-spec), so the
	// csivolumeinfo controller's ownership-transfer decision table (which
	// otherwise reads hasDependent && ownership=="" as "new dependent
	// attach, needs Unregister") MUST NOT act on this CR while the
	// annotation is present, or it will call QueryLiveDiskPath/Unregister
	// against a volume that was never registered as an FCD and fails
	// permanently with a not-found fault. The import path removes this
	// annotation only after status.ownership has been durably patched to
	// VMManaged, closing the race.
	ImportPendingAnnotation = "csi.vsphere.vmware.com/import-pending"

	// DiskPathRefreshRequestedAnnotation is set by vm-operator on a CVI whose
	// spec.diskPath it attempted to consume in a ReconfigVM_Task that failed
	// with a FileNotFound fault against that exact path — the disk was
	// relocated (e.g. storage vMotion) after CSI last resolved diskPath, so
	// the recorded value is stale. It applies to both disk modes: an
	// independent volume whose diskPath was resolved once at first sight
	// (see the csivolumeinfo controller's decision table), and a dependent
	// volume whose diskPath was captured once at ownership transfer.
	//
	// vm-operator itself never writes spec.diskPath in this case — for a
	// dependent volume, status.ownership==VMManaged is a durable invariant
	// that diskPath is non-empty (it is written by the same reconcile that
	// sets ownership), so vm-operator clearing it would violate that
	// invariant and trip vm-operator's own "empty diskPath after green
	// signal" error. This annotation is therefore the signal, orthogonal to
	// the field's current value: the csivolumeinfo controller re-resolves
	// spec.diskPath from a live query and replaces it directly with the
	// fresh value in one patch — never observably empty in between — then
	// removes this annotation. vm-operator waits for the annotation to be
	// gone (and observedGeneration to have advanced past the refresh patch)
	// before retrying the attach with the new value.
	DiskPathRefreshRequestedAnnotation = "csi.vsphere.vmware.com/diskpath-refresh-requested"
)

// OwnershipState is the current ownership of the volume.
// +kubebuilder:validation:Enum=CSIManaged;VMManaged
type OwnershipState string

const (
	// OwnershipStateCSIManaged is the steady state when the volume is a
	// registered FCD managed by CSI.
	OwnershipStateCSIManaged OwnershipState = "CSIManaged"

	// OwnershipStateVMManaged is the steady state when the disk is a plain
	// VMDK managed by a greenfield VM.
	OwnershipStateVMManaged OwnershipState = "VMManaged"
)

// PhaseState represents the reconcile phase of a CsiVolumeInfo.
// +kubebuilder:validation:Enum=Pending;Succeeded;Failed
type PhaseState string

const (
	// PhasePending indicates the controller has not yet acted on the current spec generation.
	PhasePending PhaseState = "Pending"

	// PhaseSucceeded indicates the last reconcile completed successfully.
	PhaseSucceeded PhaseState = "Succeeded"

	// PhaseFailed indicates the last reconcile encountered an error.
	PhaseFailed PhaseState = "Failed"
)

// DiskMode is the disk mode a VM attaches a volume in, mirroring vm-operator's
// VolumeDiskMode.
// +kubebuilder:validation:Enum=Persistent;IndependentPersistent;IndependentNonPersistent;NonPersistent
type DiskMode string

const (
	// DiskModePersistent is the dependent mode: CSI transfers ownership of the
	// FCD to the VM via a best-effort unregister.
	DiskModePersistent DiskMode = "Persistent"
	// DiskModeIndependentPersistent is an independent mode: the FCD stays
	// registered and CSIManaged.
	DiskModeIndependentPersistent DiskMode = "IndependentPersistent"
	// DiskModeIndependentNonPersistent is an independent mode: the FCD stays
	// registered and CSIManaged.
	DiskModeIndependentNonPersistent DiskMode = "IndependentNonPersistent"
	// DiskModeNonPersistent is treated like an independent mode for ownership
	// purposes: the FCD stays registered and CSIManaged.
	DiskModeNonPersistent DiskMode = "NonPersistent"
)

// VirtualMachineRef identifies a VM attached to the volume.
type VirtualMachineRef struct {
	// VMName is the VirtualMachine CR name.
	VMName string `json:"vmName"`
	// VMInstanceUUID is the instance UUID of the VM.
	// +optional
	VMInstanceUUID string `json:"vmInstanceUUID,omitempty"`
	// DiskMode is the disk mode this VM attaches the volume in. CSI keys the
	// ownership-transfer decision on it: a Persistent (dependent) entry triggers
	// the best-effort unregister, while an independent entry leaves the FCD
	// registered and the volume CSIManaged. Written by vm-operator, mirroring
	// vm.spec.volumes[*].diskMode. An empty value is treated as Persistent,
	// matching the vm.spec default.
	// +optional
	DiskMode DiskMode `json:"diskMode,omitempty"`
	// VolumeName is vm.spec.volumes[*].name on that VM. vm-operator writes it so a
	// detach can correlate this entry to the VM's vm.status.volumes entry — and
	// hence to the device slot — after the volume has already been removed from
	// vm.spec.volumes. CSI never reads it.
	// +optional
	VolumeName string `json:"volumeName,omitempty"`
}

// CsiVolumeInfoSpec defines the desired state of CsiVolumeInfo.
type CsiVolumeInfoSpec struct {
	// VolumeID is the CNS volume ID / PV volumeHandle. Immutable after creation.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`

	// PVCName is the name of the bound PVC. Updated in place on Retain-reclaim rebind.
	// +kubebuilder:validation:Required
	PVCName string `json:"pvcName"`

	// PVCNamespace is the namespace of the bound PVC.
	// +kubebuilder:validation:Required
	PVCNamespace string `json:"pvcNamespace"`

	// PVName is the name of the bound PersistentVolume.
	// +kubebuilder:validation:Required
	PVName string `json:"pvName"`

	// DiskUUID is the stable identifier of the virtual disk. Written by CSI after
	// UnregisterVolumeEx completes. Not cleared on release back to CSIManaged; the
	// stale value is harmless and occasionally useful for triage. Unset for an
	// fcd-retained volume, since the capture that fills it never runs on that path.
	// +optional
	DiskUUID string `json:"diskUUID,omitempty"`

	// DiskPath is the datastore path to the VMDK file. Written by CSI alongside DiskUUID
	// after UnregisterVolumeEx. May be refreshed JIT by vm-operator before attachment.
	// +optional
	DiskPath string `json:"diskPath,omitempty"`

	// VMs lists the VirtualMachine CRs that have this volume attached.
	// An empty slice means no VM currently owns the disk (CSI-managed steady state).
	// vm-operator is the sole writer of this field.
	// +optional
	// +listType=map
	// +listMapKey=vmName
	VMs []VirtualMachineRef `json:"vms,omitempty"`
}

// CsiVolumeInfoStatus defines the observed state of CsiVolumeInfo.
// Written exclusively via the /status subresource by the CsiVolumeInfo controller.
type CsiVolumeInfoStatus struct {
	// Ownership is the current ownership state of the volume.
	// +optional
	Ownership OwnershipState `json:"ownership,omitempty"`

	// Phase reflects the last reconcile outcome.
	// +optional
	Phase PhaseState `json:"phase,omitempty"`

	// ObservedGeneration is the spec.generation last acted on by the controller.
	// vm-operator waits until observedGeneration >= spec.generation before proceeding.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Error holds the last reconcile error message, if any.
	// +optional
	Error string `json:"error,omitempty"`

	// Conditions is a standard K8s condition array.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cvi,scope=Namespaced
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=`.status.ownership`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CsiVolumeInfo tracks the ownership lifecycle of a CSI volume for the VM-owned
// volume attach/detach model. One CR exists per PV in the vmware-system-csi namespace.
type CsiVolumeInfo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CsiVolumeInfoSpec   `json:"spec"`
	Status CsiVolumeInfoStatus `json:"status,omitempty"`
}

// GetConditions returns the conditions for controller-runtime conditions interface.
func (in *CsiVolumeInfo) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the conditions for controller-runtime conditions interface.
func (in *CsiVolumeInfo) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// CsiVolumeInfoList contains a list of CsiVolumeInfo.
type CsiVolumeInfoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CsiVolumeInfo `json:"items"`
}
