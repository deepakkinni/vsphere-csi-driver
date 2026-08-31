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

package csivolumeinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadEnvVar      = "WORKER_THREADS_CSIVOLUMEINFO"
	defaultMaxWorkerThreads = 10

	// conditionTypeReady is the standard condition type used on CsiVolumeInfo.
	conditionTypeReady = "Ready"

	// reason strings for conditions.
	reasonUnregisterSucceeded = "UnregisterSucceeded"
	reasonRegisterSucceeded   = "RegisterSucceeded"
	reasonReconcileFailed     = "ReconcileFailed"
	reasonInitialCSIManaged   = "InitialCSIManaged"
	reasonFcdRetained         = "FcdRetained"
	reasonFcdReleased         = "FcdReleased"
)

var (
	// backOffDuration is a map of CsiVolumeInfo names to the next requeue delay.
	// Initialised to 1 second and doubled on failure up to MaxBackOffDurationForReconciler.
	backOffDuration         map[k8stypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, volumeManager volumes.Manager,
	configInfo *commonconfig.ConfigurationInfo,
	cviSvc csivolumeinfosvc.CsiVolumeInfoService) reconcile.Reconciler {
	return &Reconciler{
		client:        mgr.GetClient(),
		apiReader:     mgr.GetAPIReader(),
		scheme:        mgr.GetScheme(),
		configInfo:    configInfo,
		volumeManager: volumeManager,
		cviSvc:        cviSvc,
	}
}

// add registers a new controller with mgr using r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadEnvVar, defaultMaxWorkerThreads)
	c := mgr.GetClient()

	err := ctrl.NewControllerManagedBy(mgr).
		Named("csivolumeinfo-controller").
		For(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		WithEventFilter(unregisterEligiblePredicate).
		Watches(&snapv1.VolumeSnapshot{},
			handler.EnqueueRequestsFromMapFunc(mapVolumeSnapshotDeleteToCVI(c)),
			builder.WithPredicates(deleteOnlyPredicate)).
		Watches(&snapv1.VolumeSnapshotContent{},
			handler.EnqueueRequestsFromMapFunc(mapVolumeSnapshotContentDeleteToCVI(c)),
			builder.WithPredicates(deleteOnlyPredicate)).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build csivolumeinfo controller. Err: %v", err)
		return err
	}

	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)
	return nil
}

// blank assignment to verify Reconciler implements reconcile.Reconciler.
var _ reconcile.Reconciler = &Reconciler{}

// Reconciler reconciles CsiVolumeInfo objects.
type Reconciler struct {
	// client is a split client: reads from cache, writes to the API server.
	client client.Client
	// apiReader reads straight from the API server, bypassing the informer
	// cache. Used only where a stale read would be unsafe rather than merely
	// slow — see clearStrayVolumeProtectionFinalizer.
	apiReader     client.Reader
	scheme        *runtime.Scheme
	configInfo    *commonconfig.ConfigurationInfo
	volumeManager volumes.Manager
	cviSvc        csivolumeinfosvc.CsiVolumeInfoService
}

// hasDependentEntry reports whether spec.vms contains at least one Persistent
// (dependent) entry. An empty DiskMode is treated as Persistent, matching
// vm.spec's own default for compatibility — but that also means a
// vm-operator build that forgets to set the field silently gets ownership
// transfer on what may be an independent disk. The webhook cannot catch this
// (an empty value is legal), so the omission is logged at Warn to keep it
// visible in the field.
func hasDependentEntry(ctx context.Context, vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
	log := logger.GetLogger(ctx)
	has := false
	for _, vm := range vms {
		if vm.DiskMode == "" {
			log.Warnf("hasDependentEntry: VM %q has no explicit diskMode; treating as Persistent", vm.VMName)
		}
		if vm.DiskMode == "" || vm.DiskMode == csivolumeinfov1alpha1.DiskModePersistent {
			has = true
		}
	}
	return has
}

// hasIndependentEntry reports whether spec.vms contains at least one
// Independent* entry.
func hasIndependentEntry(vms []csivolumeinfov1alpha1.VirtualMachineRef) bool {
	for _, vm := range vms {
		if vm.DiskMode != "" && vm.DiskMode != csivolumeinfov1alpha1.DiskModePersistent {
			return true
		}
	}
	return false
}

// hasVolumeProtectionFinalizer reports whether the CVI already carries
// VolumeProtectionFinalizer, i.e. whether GC is currently blocked for it.
//
// This answers only that question. It is NOT a signal that UnregisterVolumeEx
// has run — use hasUnregisterAttempted for that. The two were conflated
// historically, which made a finalizer left behind by a cancelled mid-attach
// impossible to remove safely: stripping it broke reconcileUnregister's
// crash-recovery path, which read the same bit as proof the destructive call
// had already completed.
func hasVolumeProtectionFinalizer(cvi *csivolumeinfov1alpha1.CsiVolumeInfo) bool {
	for _, f := range cvi.Finalizers {
		if f == csivolumeinfov1alpha1.VolumeProtectionFinalizer {
			return true
		}
	}
	return false
}

// hasUnregisterAttempted reports whether some reconcile for this CVI has
// already reached its destructive UnregisterVolumeEx call. The annotation is
// written before that call, in the same metadata patch that adds the
// finalizer, so its presence is durable and — unlike status.ownership, which
// is read through the informer cache — cannot be missed by a racing reconcile
// that started before an earlier reconcile's status write became visible.
func hasUnregisterAttempted(cvi *csivolumeinfov1alpha1.CsiVolumeInfo) bool {
	_, ok := cvi.Annotations[csivolumeinfov1alpha1.UnregisterAttemptedAnnotation]
	return ok
}

// Reconcile reads the state of the CsiVolumeInfo CR and drives the volume
// ownership state machine.  The controller is the sole writer of
// status.ownership and status.phase; vm-operator is the sole writer of
// spec.vms.
//
// Precondition: if ImportPendingAnnotation is present, the entire decision
// table below is deferred (see the annotation's doc comment) — the
// CnsRegisterVolume deferFcdRegistration ("import") path creates a CVI with
// spec.vms already populated before its status is patched to VMManaged, and
// this CR must not be mistaken for a normal new dependent attach in that
// window (it has no backing FCD to Unregister).
//
// Decision table (hasDependent = any Persistent entry in spec.vms; empty ⇒ Persistent):
//
//	hasDependent  ∧ (ownership=="" ∨ ownership=="CSIManaged")        → reconcileUnregister
//	ownership=="VMManaged" ∧ fcd-retained ∧ len(spec.vms)>0          → convergence (C8; no-op here)
//	!hasDependent ∧ ownership!="VMManaged" ∧ unregister-attempted    → converge status to VMManaged, requeue
//	!hasDependent ∧ ownership=="VMManaged"                           → reconcileRegister
//	vmCount>0 ∧ !hasDependent ∧ ownership=="CSIManaged" ∧ diskPath==""→ reconcileIndependentDiskPath (one-time)
//	len(spec.vms)>0 ∧ !hasDependent ∧ ownership=="CSIManaged"        → idle (independent-only; CSI never owns it)
//	len(spec.vms)==0 ∧ ownership==""                                 → initial CSIManaged
//	otherwise                                                        → idle (no-op)
//
// Independent-only and no-entries are kept as separate cases rather than
// collapsed into one "idle for ownership" bucket: both mean CSI is idle, but
// only the second (no entries) permits release.
//
// The two unregister-attempted rows above (the table row, and
// clearStrayVolumeProtectionFinalizer which runs before the table) exist for
// the same reason and split the same way. An attach can be cancelled at any
// point during ownership transfer, and the marker is what says which side of
// the destructive call it was cancelled on: before it, the finalizer is
// stray and gets stripped; after it, the FCD really is gone and status has
// to catch up to that before the register path can undo it. Neither state is
// reachable by the rest of the table, which only ever sees consistent
// (spec.vms, status.ownership) pairs.
//
// spec.diskPath is a snapshot taken just-in-time at the moment it is first
// needed, not a continuously-refreshed live mirror — the same convention
// reconcileUnregister already follows for a dependent attach (it calls
// QueryLiveDiskPath immediately before consuming the value, not at some
// earlier point) and the detach path follows for the remove (refreshed
// again right before the device comes off). An independent volume's path is
// captured once, here, the first time CSI observes it; vm-operator is
// expected to consume it promptly afterward. See csi.md §9 for the fuller
// staleness discussion (why this is a snapshot, not a live mirror, and what
// happens if the FCD relocates between the write and the consuming attach).
//
// DiskPathRefreshRequestedAnnotation (checked before the table above, since
// it applies across both hasDependent states) is how vm-operator recovers
// from exactly that relocation: on a ReconfigVM_Task that fails with
// FileNotFound against the path it read, it sets the annotation instead of
// touching spec.diskPath itself (see the annotation's doc comment for why —
// in short, clearing it would violate the ownership==VMManaged invariant).
// reconcileDiskPathRefresh re-resolves and replaces the value directly, for
// either mode, once the volume is in a state where a plain re-resolve is
// safe (independent/CSIManaged, or dependent/VMManaged without
// fcd-retained) — otherwise the annotation is left for a later reconcile.
func (r *Reconciler) Reconcile(ctx context.Context,
	req reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", req.NamespacedName)
	log.Infof("Reconcile: entry for CsiVolumeInfo %s", req.Name)

	// CVI CRs always live in vmware-system-csi; look up by the fixed namespace.
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	nn := k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      req.Name,
	}
	if err := r.client.Get(ctx, nn, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			// The CVI is gone; drop its backoff entry so the map does not retain
			// state for deleted volumes.
			deleteBackoffEntry(ctx, nn)
			log.Infof("Reconcile: CsiVolumeInfo %s not found; must be deleted — no action", req.Name)
			return reconcile.Result{}, nil
		}
		log.Errorf("Reconcile: error reading CsiVolumeInfo %s: %v", req.Name, err)
		return reconcile.Result{}, err
	}

	backoff := getBackoffDuration(ctx, nn)
	log.Infof("Reconcile: current backoff duration %s", backoff)

	// Ensure the PV ownerRef is present. This is idempotent and repairs CVIs
	// created before this logic existed (existing CVIs are reconciled on startup
	// via synthetic Create events from the informer cache sync).
	//
	// The CVI is created before the PV exists (wcp/controller.go createBlockVolume),
	// so an error+requeue here on the first few reconciles is expected steady-state
	// while the PV catches up, not a defect.
	if err := r.ensurePVOwnerRef(ctx, cvi); err != nil {
		log.Errorf("Reconcile: failed to ensure PV ownerRef for %s: %v", req.Name, err)
		return reconcile.Result{}, err
	}

	// usedby-vm annotations and the pvc-volume-protection finalizer are
	// orthogonal to the ownership state machine below — they key on
	// spec.vms being non-empty, not on status.ownership, so this runs
	// unconditionally on every branch including the idle ones.
	if err := r.syncPVCUsedByAndProtection(ctx, cvi); err != nil {
		log.Errorf("Reconcile: failed to sync PVC usedby-vm/finalizer for %s: %v", req.Name, err)
		return reconcile.Result{}, err
	}

	// The CnsRegisterVolume deferFcdRegistration ("import") path creates a CVI
	// with spec.vms already populated, then patches status to VMManaged in a
	// second, separate call. In between, this CR looks identical to a normal
	// new dependent attach (hasDependent && ownership==""), but it has no
	// backing FCD by design — Unregister-ing it fails permanently. Defer the
	// entire ownership state machine until the import path removes this
	// annotation (which it only does after status is durably VMManaged).
	if _, importPending := cvi.Annotations[csivolumeinfov1alpha1.ImportPendingAnnotation]; importPending {
		log.Infof("Reconcile: %s has %s annotation; import in progress, deferring", req.Name,
			csivolumeinfov1alpha1.ImportPendingAnnotation)
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}

	vmCount := len(cvi.Spec.VMs)
	ownership := cvi.Status.Ownership
	hasDependent := hasDependentEntry(ctx, cvi.Spec.VMs)
	_, fcdRetained := cvi.Annotations[csivolumeinfov1alpha1.FcdRetainedAnnotation]
	log.Infof("Reconcile: vmCount=%d hasDependent=%t ownership=%q fcdRetained=%t for %s",
		vmCount, hasDependent, ownership, fcdRetained, req.Name)

	// Self-correction: the fcd-retained annotation is authoritative for
	// whether a volume is deferred. If it's gone but the Ready condition
	// still carries reasonFcdRetained, some prior reconcile cleared the
	// annotation (attemptConvergence) but crashed, was interrupted, or
	// otherwise errored before its own status patch landed — or the
	// condition was left stale by an older build without that patch. Fix it
	// here rather than leaving a "N FCD snapshots present" message stuck on
	// a volume that has already converged.
	if !fcdRetained && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged &&
		readyConditionReason(cvi) == reasonFcdRetained {
		log.Infof("Reconcile: %s has stale FcdRetained condition with no fcd-retained annotation; "+
			"correcting to FcdReleased", req.Name)
		patch := buildStatusPatch(cvi.Generation, ownership, csivolumeinfov1alpha1.PhaseSucceeded, "",
			"unregister blocker cleared; volume is fully VM-managed", reasonFcdReleased, true)
		if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch); err != nil {
			log.Errorf("Reconcile: failed to correct stale FcdRetained condition for %s: %v", req.Name, err)
			return reconcile.Result{}, err
		}
	}

	// Self-correction: strip a volume-protection finalizer left behind by an
	// attach that was cancelled before ownership transfer began. Nothing in
	// the decision table below can clean this up (see the helper's comment),
	// so it has to happen here, before the table runs.
	requeue, err := r.clearStrayVolumeProtectionFinalizer(ctx, cvi, hasDependent)
	if err != nil {
		log.Errorf("Reconcile: failed to clear stray volume-protection finalizer for %s: %v",
			req.Name, err)
		return reconcile.Result{}, err
	}
	if requeue {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}

	// vm-operator requests a diskPath refresh when a ReconfigVM_Task it
	// issued failed with FileNotFound against the exact path it read from
	// spec.diskPath — the disk was relocated after CSI last resolved it.
	// This is orthogonal to the decision table below: it applies whether
	// the volume is independent (CSIManaged) or dependent (VMManaged), and
	// unlike the "resolve when empty" branch for a brand-new independent
	// volume, it must NOT go through an empty intermediate value for a
	// dependent volume — ownership==VMManaged is a durable invariant that
	// diskPath is non-empty. Only act once the volume is in one of the two
	// states where a plain re-resolve (no unregister, no ownership change)
	// is safe; otherwise leave the annotation for a later reconcile once
	// the volume settles (e.g. mid-transfer).
	if _, refreshRequested := cvi.Annotations[csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation]; refreshRequested {
		eligible := (vmCount > 0 && !hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateCSIManaged) ||
			(hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged && !fcdRetained)
		if eligible {
			log.Infof("Reconcile: %s has %s annotation and is eligible for a plain refresh "+
				"(hasDependent=%t ownership=%q) → reconcileDiskPathRefresh", req.Name,
				csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, hasDependent, ownership)
			if err := r.reconcileDiskPathRefresh(ctx, cvi, hasDependent); err != nil {
				log.Errorf("Reconcile: reconcileDiskPathRefresh failed: %v", err)
				doubleBackoffDuration(ctx, nn)
				return reconcile.Result{RequeueAfter: backoff}, nil
			}
			updateBackoffEntry(ctx, nn, time.Second)
			log.Infof("Reconcile: exit for CsiVolumeInfo %s", req.Name)
			return reconcile.Result{}, nil
		}
		// Not eligible. Distinguish "not yet" from "never", because deferring
		// unconditionally is an unbounded stall, not a wait: this branch
		// returns before the decision table below, so nothing else can act on
		// the volume while the annotation is set.
		//
		// A volume with no dependent VM attached and ownership==VMManaged is
		// the "never" case. There is no VM device list to re-resolve the path
		// from, so the refresh is unserviceable — and it is also moot, because
		// the only thing left to do with this volume is release it, which the
		// decision table does via reconcileRegister. Deferring instead trapped
		// it: reconciled once a second forever, ownership stuck at VMManaged,
		// the volume-protection finalizer never dropped, and its PVC and PV
		// therefore permanently undeletable. Observed in the field at exactly
		// 1 Hz on volumes whose VM had already been deleted.
		//
		// Clear the annotation and fall through so the release can proceed.
		if !hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged {
			log.Infof("Reconcile: %s has %s annotation but no dependent VM is attached "+
				"(ownership=%q), so there is no device list to re-resolve from and nothing to refresh "+
				"for — clearing the annotation so the volume can be released", req.Name,
				csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, ownership)
			if err := r.clearDiskPathRefreshRequested(ctx, cvi); err != nil {
				log.Errorf("Reconcile: failed to clear unserviceable %s for %s: %v",
					csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, req.Name, err)
				doubleBackoffDuration(ctx, nn)
				return reconcile.Result{RequeueAfter: backoff}, nil
			}
			// Fall through to the decision table with the annotation gone.
		} else {
			// Genuinely transient — e.g. mid-transfer, where ownership is
			// still settling and a later reconcile will find it eligible.
			log.Infof("Reconcile: %s has %s annotation but is not yet in an eligible state "+
				"(hasDependent=%t ownership=%q fcdRetained=%t); deferring", req.Name,
				csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, hasDependent, ownership, fcdRetained)
			return reconcile.Result{RequeueAfter: time.Second}, nil
		}
	}

	// Defense in depth for the single-mode-per-volume invariant: the webhook
	// is the primary enforcement point, this is the fallback for a webhook
	// outage. A mixed spec.vms fails legibly rather than guessing which
	// disk-mode behavior applies.
	if hasDependent && hasIndependentEntry(cvi.Spec.VMs) {
		log.Errorf("Reconcile: CsiVolumeInfo %s has mixed disk modes in spec.vms (both Persistent and "+
			"Independent* entries); failing", req.Name)
		if statusErr := r.setFailedStatus(ctx, cvi,
			"spec.vms mixes a Persistent entry with an Independent* entry"); statusErr != nil {
			log.Warnf("Reconcile: could not write failed status for mixed disk modes: %v", statusErr)
		}
		return reconcile.Result{}, nil
	}

	switch {
	case hasDependent && (ownership == "" || ownership == csivolumeinfov1alpha1.OwnershipStateCSIManaged):
		log.Infof("Reconcile: %d VM(s) attached (dependent), ownership=%q → reconcileUnregister", vmCount, ownership)
		if err := r.reconcileUnregister(ctx, cvi); err != nil {
			var transientErr *transientUnregisterBlockError
			if errors.As(err, &transientErr) {
				// A transient blocker (e.g. an unreachable host) is not a
				// failure: the volume is untouched, so status is left alone
				// and only the backoff advances.
				log.Infof("Reconcile: unregister deferred by a transient blocker for %s; "+
					"requeueing without a status change: %v", req.Name, err)
				doubleBackoffDuration(ctx, nn)
				return reconcile.Result{RequeueAfter: backoff}, nil
			}
			log.Errorf("Reconcile: reconcileUnregister failed: %v", err)
			if statusErr := r.setFailedStatus(ctx, cvi, err.Error()); statusErr != nil {
				log.Warnf("Reconcile: could not write failed status: %v", statusErr)
			}
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case vmCount > 0 && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged && fcdRetained:
		// Still attached, still deferred: woken by a spec change or the
		// VolumeSnapshot/VolumeSnapshotContent delete watch, re-attempt the
		// unregister. Detach (the release case below) remains the backstop
		// if it never converges.
		log.Infof("Reconcile: volume %q is fcd-retained with %d VM(s) still attached → attemptConvergence",
			cvi.Spec.VolumeID, vmCount)
		if err := r.attemptConvergence(ctx, cvi); err != nil {
			var transientErr *transientUnregisterBlockError
			if errors.As(err, &transientErr) {
				log.Infof("Reconcile: convergence re-attempt deferred by a transient blocker for %s; "+
					"requeueing without a status change: %v", req.Name, err)
				doubleBackoffDuration(ctx, nn)
				return reconcile.Result{RequeueAfter: backoff}, nil
			}
			log.Errorf("Reconcile: attemptConvergence failed: %v", err)
			if statusErr := r.setFailedStatus(ctx, cvi, err.Error()); statusErr != nil {
				log.Warnf("Reconcile: could not write failed status: %v", statusErr)
			}
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case !hasDependent && ownership != csivolumeinfov1alpha1.OwnershipStateVMManaged &&
		hasUnregisterAttempted(cvi):
		// The mirror image of the stray-finalizer case above: here the
		// destructive call DID happen (the marker is on record) but the detach
		// landed before the status flip, so the FCD is gone while
		// status.ownership still reads "" or CSIManaged. Left alone, this
		// falls through to the initial-CSIManaged or idle branch and the CVI
		// permanently misreports a volume that is now a plain VMDK — and
		// reconcileRegister, the only thing that can turn it back into an
		// FCD, is unreachable because it requires ownership==VMManaged.
		//
		// Converge to VMManaged first and let the next pass take the
		// reconcileRegister branch below. Splitting it over two passes rather
		// than calling reconcileRegister inline keeps a single meaning for
		// each state: this branch only repairs status to match what already
		// happened on the storage side, and the register path stays the one
		// place that re-registers.
		log.Infof("Reconcile: no dependent VM(s) attached and ownership=%q, but an unregister is on "+
			"record for %s — converging status to VMManaged so the register path can pick it up",
			ownership, req.Name)
		// Order matters: the finalizer must be in place before status reads
		// VMManaged. Idempotent, and the marker is already set.
		if err := r.cviSvc.MarkUnregisterInFlight(ctx, cvi.Spec.VolumeID); err != nil {
			log.Errorf("Reconcile: failed to re-assert protection finalizer for %s: %v", req.Name, err)
			return reconcile.Result{}, err
		}
		patch := buildStatusPatch(cvi.Generation,
			csivolumeinfov1alpha1.OwnershipStateVMManaged,
			csivolumeinfov1alpha1.PhaseSucceeded, "", "", reasonUnregisterSucceeded, true)
		if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch); err != nil {
			log.Errorf("Reconcile: failed to converge status to VMManaged for %s: %v", req.Name, err)
			return reconcile.Result{}, err
		}
		// A status-only write is dropped by unregisterEligiblePredicate, so
		// this reconcile has to schedule its own follow-up.
		log.Infof("Reconcile: exit for CsiVolumeInfo %s (requeued for register)", req.Name)
		return reconcile.Result{RequeueAfter: time.Second}, nil

	case !hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged:
		log.Infof("Reconcile: no dependent VM(s) attached, ownership=VMManaged → reconcileRegister")
		if err := r.reconcileRegister(ctx, cvi); err != nil {
			log.Errorf("Reconcile: reconcileRegister failed: %v", err)
			if statusErr := r.setFailedStatus(ctx, cvi, err.Error()); statusErr != nil {
				log.Warnf("Reconcile: could not write failed status: %v", statusErr)
			}
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case vmCount > 0 && !hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateCSIManaged && cvi.Spec.DiskPath == "":
		// Independent-only, but spec.diskPath has never been resolved: unlike
		// a dependent attach (whose diskPath is captured as a side effect of
		// reconcileUnregister), an independent volume never goes through that
		// call — CSI is otherwise idle for it — so nothing has ever queried
		// the FCD's backing path. vm-operator's own attach needs that path
		// (it has no CNS/vslm client of its own, by design) and would
		// otherwise wait on it forever. This is the one-time resolution;
		// once diskPath is set, the case below is a true no-op.
		log.Infof("Reconcile: %d independent VM(s) attached, ownership=CSIManaged, diskPath unset → "+
			"reconcileIndependentDiskPath", vmCount)
		if err := r.reconcileIndependentDiskPath(ctx, cvi); err != nil {
			log.Errorf("Reconcile: reconcileIndependentDiskPath failed: %v", err)
			doubleBackoffDuration(ctx, nn)
			return reconcile.Result{RequeueAfter: backoff}, nil
		}
		updateBackoffEntry(ctx, nn, time.Second)

	case vmCount > 0 && !hasDependent && ownership == csivolumeinfov1alpha1.OwnershipStateCSIManaged:
		// Independent-only: the FCD stays registered and CSI-managed for the
		// life of the attachment. C9 (usedby-vm) and C10's PVC finalizer key
		// on spec.vms being non-empty, not on this branch, so they still run
		// here once implemented — this case exists so that fact is visible
		// in the log rather than falling through to the generic idle case.
		log.Infof("Reconcile: %d independent VM(s) attached, ownership=CSIManaged; idle for ownership", vmCount)

	case ownership == "":
		// Initial state: no status written yet. By this point hasDependent is
		// known false (a dependent attach with ownership=="" already matched the
		// first case above), so this covers both "CR just created, no VMs
		// attached yet" and "an independent-only VM attached before the
		// controller's first reconcile could write the initial status" (the
		// latter previously fell through to the default idle case and left
		// status permanently empty). Write the initial CSIManaged status so
		// vm-operator's observedGeneration wait condition is satisfied.
		log.Infof("Reconcile: initial state — writing CSIManaged status for %s", req.Name)
		patch := buildStatusPatch(cvi.Generation,
			csivolumeinfov1alpha1.OwnershipStateCSIManaged,
			csivolumeinfov1alpha1.PhaseSucceeded,
			"", "", reasonInitialCSIManaged, true)
		if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch); err != nil {
			log.Errorf("Reconcile: failed to set initial CSIManaged status: %v", err)
			return reconcile.Result{}, err
		}

	default:
		log.Infof("Reconcile: idle — vmCount=%d, ownership=%q; no action", vmCount, ownership)
		// Idle means "this spec needs no action from CSI" — which still has to
		// be *reported*, because vm-operator's green-signal check is
		// status.observedGeneration >= metadata.generation. vm-operator writes
		// spec.vms itself (it fills in diskMode/volumeName on an existing
		// entry after the initial attach request), and any such write bumps
		// generation after our last status patch. Without this, the CVI sits
		// at generation=N+1 / observedGeneration=N forever: no branch above
		// matches a settled dependent+VMManaged volume, so nothing ever
		// republishes status, the green signal never returns true, and
		// vm-operator waits on an attach it will never start.
		if err := r.recordObservedGeneration(ctx, cvi); err != nil {
			log.Errorf("Reconcile: failed to record observedGeneration for %s: %v", req.Name, err)
			return reconcile.Result{}, err
		}
	}

	log.Infof("Reconcile: exit for CsiVolumeInfo %s", req.Name)
	return reconcile.Result{}, nil
}

// disposition is the severity of an unregister blocker, ordered
// Permanent > Structural > Transient.
type disposition int

const (
	dispositionTransient disposition = iota
	dispositionStructural
	dispositionPermanent
)

func (d disposition) String() string {
	switch d {
	case dispositionPermanent:
		return "PERMANENT"
	case dispositionStructural:
		return "STRUCTURAL"
	default:
		return "TRANSIENT"
	}
}

// worstDisposition returns the most severe disposition among the reported
// blockers, using the ordering Permanent > Structural > Transient.
//
// Two defaults are load-bearing. An unrecognized disposition is treated as
// Structural: deferring an unfamiliar blocker is safe, retrying one forever
// is not. An empty blocker list is treated as Transient: either the blocker
// cleared between the attempt and the query, or the fault was unrelated to
// feasibility. (A feasibility query that itself could not be evaluated is a
// separate case, handled by the caller before this function is reached.)
func worstDisposition(blockers []cnstypes.CnsUnregisterBlocker) disposition {
	worst := dispositionTransient
	for _, b := range blockers {
		var d disposition
		switch b.Disposition {
		case cnstypes.CnsUnregisterBlockerDispositionPermanent:
			d = dispositionPermanent
		case cnstypes.CnsUnregisterBlockerDispositionTransient:
			d = dispositionTransient
		default: // CnsUnregisterBlockerDispositionStructural and any unrecognized value.
			d = dispositionStructural
		}
		if d > worst {
			worst = d
		}
	}
	return worst
}

// blockerMessage renders the blockers reported by the feasibility query into
// a status condition message naming the blocking condition, so a CBT block
// can be told apart from a snapshot block without resorting to logs.
func blockerMessage(blockers []cnstypes.CnsUnregisterBlocker) string {
	if len(blockers) == 0 {
		return "unregister blocked; blocker detail unavailable"
	}
	parts := make([]string, 0, len(blockers))
	for _, b := range blockers {
		if b.Detail != "" {
			parts = append(parts, fmt.Sprintf("%s: %s", b.Condition, b.Detail))
		} else {
			parts = append(parts, b.Condition)
		}
	}
	return strings.Join(parts, "; ")
}

// transientUnregisterBlockError signals that an unregister attempt was
// blocked by a transient precondition (e.g. an unreachable host). The volume
// is neither VMManaged nor Failed; Reconcile requeues with backoff and leaves
// status untouched, because nothing about the volume has actually failed.
type transientUnregisterBlockError struct {
	cause error
}

func (e *transientUnregisterBlockError) Error() string { return e.cause.Error() }
func (e *transientUnregisterBlockError) Unwrap() error { return e.cause }

// handleUnregisterBlocked classifies a failed UnregisterVolumeEx attempt via
// the feasibility query and either defers the volume as fcd-retained
// (Permanent or Structural) or signals a transient block to the caller for a
// backoff-only retry. patchedGen is the generation from the live-path patch
// already applied earlier in reconcileUnregister; a Permanent/Structural
// defer reuses it because the fcd-retained annotation patch that precedes the
// status write is metadata-only and does not advance generation.
func (r *Reconciler) handleUnregisterBlocked(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo,
	patchedGen int64, faultType string, unregErr error) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	log.Warnf("reconcileUnregister: UnregisterVolumeEx blocked for volume %q (fault=%q): %v",
		cvi.Spec.VolumeID, faultType, unregErr)

	log.Infof("reconcileUnregister: calling QueryUnregisterFeasibility for volume %q", cvi.Spec.VolumeID)
	feasibilities, err := r.volumeManager.QueryUnregisterFeasibility(ctx, []string{cvi.Spec.VolumeID})

	var disp disposition
	var blockers []cnstypes.CnsUnregisterBlocker
	switch {
	case err != nil:
		log.Warnf("reconcileUnregister: QueryUnregisterFeasibility call failed for volume %q; "+
			"treating as structural: %v", cvi.Spec.VolumeID, err)
		disp = dispositionStructural
	case len(feasibilities) != 1:
		log.Warnf("reconcileUnregister: QueryUnregisterFeasibility returned %d results for volume %q, "+
			"expected 1; treating as structural", len(feasibilities), cvi.Spec.VolumeID)
		disp = dispositionStructural
	case feasibilities[0].EvaluationFault != "":
		log.Warnf("reconcileUnregister: QueryUnregisterFeasibility could not evaluate volume %q "+
			"(fault=%q); treating as structural", cvi.Spec.VolumeID, feasibilities[0].EvaluationFault)
		disp = dispositionStructural
	default:
		blockers = feasibilities[0].Blockers
		disp = worstDisposition(blockers)
	}
	log.Infof("reconcileUnregister: worst disposition for volume %q is %s (blockers=%d)",
		cvi.Spec.VolumeID, disp, len(blockers))

	if disp == dispositionTransient {
		log.Infof("reconcileUnregister: transient block for volume %q; deferring to backoff "+
			"without a status change", cvi.Spec.VolumeID)
		return &transientUnregisterBlockError{cause: unregErr}
	}
	return r.deferAsFcdRetained(ctx, cvi, patchedGen, blockers)
}

// deferAsFcdRetained marks the volume fcd-retained and flips it to
// VMManaged/Succeeded — a functional, not failed, resting state. Patch
// ordering is a correctness requirement: the fcd-retained metadata patch
// lands before the status flip, so no observer ever sees
// ownership=VMManaged on a retained FCD without the annotation that marks it
// locked down.
func (r *Reconciler) deferAsFcdRetained(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo,
	patchedGen int64, blockers []cnstypes.CnsUnregisterBlocker) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	message := blockerMessage(blockers)
	log.Infof("reconcileUnregister: deferring volume %q as fcd-retained: %s", cvi.Spec.VolumeID, message)

	annotationPatch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				csivolumeinfov1alpha1.FcdRetainedAnnotation: "true",
			},
		},
	})
	if err != nil {
		return fmt.Errorf("reconcileUnregister: failed to marshal fcd-retained annotation patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, annotationPatch); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch fcd-retained annotation for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: fcd-retained annotation set for volume %q", cvi.Spec.VolumeID)

	statusPatch := buildStatusPatch(patchedGen,
		csivolumeinfov1alpha1.OwnershipStateVMManaged,
		csivolumeinfov1alpha1.PhaseSucceeded, "", message, reasonFcdRetained, true)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch fcd-retained status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: status patched to VMManaged/Succeeded (FcdRetained) for volume %q",
		cvi.Spec.VolumeID)
	return nil
}

// reconcileUnregister captures the volume's live disk identity, then attempts
// a best-effort unregister, and transitions the CVI to VMManaged ownership.
//
// Steps:
//  1. Call volumeManager.QueryLiveDiskPath and patch spec.diskPath with the
//     result. Add the volume-protection finalizer. This durably records the
//     disk's location before the destructive call below, so a crash anywhere
//     after this point is recoverable from the CVI alone.
//  2. Call volumeManager.UnregisterVolumeEx.
//  3. On success, patch spec.diskPath and spec.diskUUID from the result — this
//     supersedes step 1's value. (Fault classification is C5's job.)
//  4. Patch status: ownership=VMManaged, phase=Succeeded, observedGeneration, Ready=True.
func (r *Reconciler) reconcileUnregister(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	log.Infof("reconcileUnregister: calling QueryLiveDiskPath for volume %q", cvi.Spec.VolumeID)

	livePath, err := r.volumeManager.QueryLiveDiskPath(ctx, cvi.Spec.VolumeID)
	if err != nil {
		// A second, racing reconcile can land here after an earlier reconcile
		// already ran UnregisterVolumeEx to completion on this same CVI: two
		// spec patches below (live-path, then post-unregister) each advance
		// metadata.generation and enqueue a fresh reconcile, and that reconcile
		// can start, and read a client-cache snapshot of status.ownership that
		// predates the first reconcile's own status write, before the cache
		// observes it. That reconcile re-enters this same branch and calls
		// QueryLiveDiskPath again — but the disk is no longer a first-class
		// disk (FCD) to query; UnregisterVolumeEx is a one-way transition, so
		// this is a permanent NotFound, not a transient one.
		//
		// UnregisterAttemptedAnnotation is only ever added a few lines below,
		// once, right before that destructive call — its presence on the
		// object we were handed is a durable signal (unlike status.ownership
		// here, not subject to the same cache race) that some earlier
		// reconcile for this CVI already got that far. If so, treat this
		// failure as confirmation that unregister already succeeded rather
		// than a genuine error, and converge straight to VMManaged/Succeeded
		// using the diskPath/diskUUID that earlier reconcile already durably
		// recorded on spec — no further destructive calls are needed or safe
		// to make.
		//
		// The annotation is the signal, not VolumeProtectionFinalizer: the
		// finalizer is also (correctly) removed by the release path and by
		// clearStrayVolumeProtectionFinalizer, so reading it as the primary
		// signal would let an unrelated, legitimate finalizer removal erase
		// this branch's evidence and send the reconcile back into the
		// destructive call — or, worse, let it converge to VMManaged with no
		// protection finalizer at all.
		//
		// The finalizer is still accepted as a fallback, for one release, to
		// cover CVIs written before UnregisterAttemptedAnnotation existed. The
		// exposure is narrow but real: a CVI caught exactly mid-transfer by
		// the upgrade (unregister completed, status flip not yet landed —
		// precisely the window a syncer restart creates) carries a finalizer
		// and no annotation, and gating on the annotation alone would send it
		// into permanent backoff with phase=Failed on a disk that is already
		// VM-owned. That is the exact failure this convergence branch was
		// written to prevent, so it must not regress across the upgrade that
		// introduces the fix.
		//
		// Accepting the finalizer here is safe in a way that accepting it in
		// clearStrayVolumeProtectionFinalizer would not be: this branch is
		// only reachable with hasDependent true, where the stray-clear never
		// fires, so the two can never race over the same bit. Drop the
		// fallback once no pre-annotation CVIs can remain in the field.
		if hasUnregisterAttempted(cvi) || hasVolumeProtectionFinalizer(cvi) {
			log.Infof("reconcileUnregister: QueryLiveDiskPath failed for %q but an unregister is already "+
				"on record (attempted-annotation=%t legacy-finalizer-fallback=%t), implying an earlier "+
				"reconcile already completed UnregisterVolumeEx for this volume; converging to "+
				"VMManaged/Succeeded without retrying the destructive call: %v",
				cvi.Spec.VolumeID, hasUnregisterAttempted(cvi),
				!hasUnregisterAttempted(cvi) && hasVolumeProtectionFinalizer(cvi), err)
			// Re-assert the finalizer before the status flip. Under the old
			// gating this was implied — the finalizer was the gate, so it was
			// necessarily present. Now that the annotation is the gate, the
			// two can in principle be observed apart, and the invariant
			// "status.ownership==VMManaged ⇒ protection finalizer present"
			// must be upheld explicitly rather than assumed. Idempotent.
			if markErr := r.cviSvc.MarkUnregisterInFlight(ctx, cvi.Spec.VolumeID); markErr != nil {
				return fmt.Errorf("reconcileUnregister: failed to re-assert protection finalizer for %q "+
					"on already-unregistered convergence: %w", cvi.Spec.VolumeID, markErr)
			}
			statusPatch := buildStatusPatch(cvi.Generation,
				csivolumeinfov1alpha1.OwnershipStateVMManaged,
				csivolumeinfov1alpha1.PhaseSucceeded, "", "", reasonUnregisterSucceeded, true)
			if statusErr := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); statusErr != nil {
				return fmt.Errorf("reconcileUnregister: failed to patch status for %q on "+
					"already-unregistered convergence: %w", cvi.Spec.VolumeID, statusErr)
			}
			log.Infof("reconcileUnregister: status patched to VMManaged/Succeeded for volume %q "+
				"(already-unregistered convergence)", cvi.Spec.VolumeID)
			return nil
		}
		// Includes NotFound, which the manager's live query can return
		// transiently right after a storage vMotion to a different datastore.
		// That is retryable, not evidence the disk is gone, so the unregister
		// below must not be attempted without a path.
		return fmt.Errorf("reconcileUnregister: QueryLiveDiskPath failed for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: QueryLiveDiskPath succeeded for volume %q — diskPath=%q",
		cvi.Spec.VolumeID, livePath)

	livePathPatch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"diskPath": livePath,
		},
	})
	if err != nil {
		return fmt.Errorf("reconcileUnregister: failed to marshal live-path spec patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	// The spec patch increments metadata.generation, so capture the post-patch
	// generation. observedGeneration must reflect this latest generation;
	// otherwise vm-operator's green-signal check (observedGeneration >= generation)
	// would never be satisfied, because the controller's own spec write advanced
	// generation beyond the value observed at reconcile entry. There are two
	// spec patches in this function; the second (step 3) supersedes this value.
	patchedGen, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, livePathPatch)
	if err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch live spec.diskPath for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: patched live spec.diskPath for volume %q (generation=%d)",
		cvi.Spec.VolumeID, patchedGen)

	// Add the volume-protection finalizer, and the unregister-attempted
	// annotation that records this reconcile is about to make the destructive
	// call, before making it: GC blocked, and the fact durably recorded for
	// any later reconcile (including one on a restarted controller) that has
	// to decide whether the FCD is already gone. One patch for both, so a
	// crash cannot leave a finalizer without its annotation. Metadata changes
	// do not advance generation, so patchedGen remains the generation to
	// record in status.
	if err := r.cviSvc.MarkUnregisterInFlight(ctx, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to mark unregister in flight for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: volume-protection finalizer and unregister-attempted annotation "+
		"added for volume %q", cvi.Spec.VolumeID)

	log.Infof("reconcileUnregister: calling UnregisterVolumeEx for volume %q", cvi.Spec.VolumeID)
	backingDiskPath, diskUUID, faultType, unregErr := r.volumeManager.UnregisterVolumeEx(ctx, cvi.Spec.VolumeID)
	if unregErr != nil {
		// A blocked unregister is not necessarily a failure — it may defer to
		// fcd-retained (functional) or need a plain backoff retry (transient).
		// Classification is handleUnregisterBlocked's job.
		return r.handleUnregisterBlocked(ctx, cvi, patchedGen, faultType, unregErr)
	}
	log.Infof("reconcileUnregister: UnregisterVolumeEx succeeded — diskPath=%q, diskUUID=%q",
		backingDiskPath, diskUUID)

	if backingDiskPath != "" && (backingDiskPath != livePath || diskUUID != cvi.Spec.DiskUUID) {
		// Persist diskPath and diskUUID from the unregister result; this
		// supersedes the live path patched in step 1. Skipped when the result
		// is identical to what step 1 already wrote (the common case — the
		// disk does not move between the two calls a few hundred milliseconds
		// apart) so this function issues only one generation-advancing spec
		// write instead of two. Each such write enqueues a fresh reconcile for
		// this object; halving them cuts in half how often a racing reconcile
		// can observe a stale (pre-status-write) ownership from the client
		// cache and re-enter this function against an already-unregistered
		// disk — the case the QueryLiveDiskPath NotFound convergence above
		// exists to catch, but avoiding the race in the first place is
		// cheaper than recovering from it every time.
		specPatch, err := json.Marshal(map[string]interface{}{
			"spec": map[string]interface{}{
				"diskPath": backingDiskPath,
				"diskUUID": diskUUID,
			},
		})
		if err != nil {
			return fmt.Errorf("reconcileUnregister: failed to marshal spec patch for %q: %w",
				cvi.Spec.VolumeID, err)
		}
		patchedGen, err = r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, specPatch)
		if err != nil {
			return fmt.Errorf("reconcileUnregister: failed to patch spec for %q: %w",
				cvi.Spec.VolumeID, err)
		}
		log.Infof("reconcileUnregister: patched spec.diskPath and spec.diskUUID for volume %q (generation=%d)",
			cvi.Spec.VolumeID, patchedGen)
	} else if backingDiskPath == "" {
		// An empty backingDiskPath means UnregisterVolumeEx found the volume
		// already unregistered (the documented idempotent outcome). Keep the
		// live-captured spec.diskPath from step 1 rather than overwriting it
		// with an empty value.
		log.Infof("reconcileUnregister: volume %q was already unregistered; keeping the live-captured spec.diskPath",
			cvi.Spec.VolumeID)
	} else {
		// backingDiskPath/diskUUID are non-empty but identical to what step 1
		// already wrote; skip the redundant spec write (see comment above).
		log.Infof("reconcileUnregister: UnregisterVolumeEx result matches the live-captured spec.diskPath "+
			"for volume %q; skipping redundant spec patch", cvi.Spec.VolumeID)
	}

	// Write status: ownership=VMManaged, phase=Succeeded.
	statusPatch := buildStatusPatch(patchedGen,
		csivolumeinfov1alpha1.OwnershipStateVMManaged,
		csivolumeinfov1alpha1.PhaseSucceeded, "", "", reasonUnregisterSucceeded, true)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); err != nil {
		return fmt.Errorf("reconcileUnregister: failed to patch status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileUnregister: status patched to VMManaged/Succeeded for volume %q",
		cvi.Spec.VolumeID)
	return nil
}

// reconcileIndependentDiskPath resolves and records spec.diskPath for an
// independent-mode volume the one time it is needed: an independent attach
// never runs reconcileUnregister (the only other place a live path gets
// captured), and CSI otherwise stays fully idle for an independent entry —
// nothing else in this controller ever queries the FCD's backing. This is a
// read-only, non-destructive live query; it does not touch ownership, add
// any finalizer, or transfer the FCD, so a transient failure here is safely
// retried without altering the CVI's status.
func (r *Reconciler) reconcileIndependentDiskPath(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)
	log.Infof("reconcileIndependentDiskPath: calling QueryLiveDiskPath for volume %q", cvi.Spec.VolumeID)

	livePath, err := r.volumeManager.QueryLiveDiskPath(ctx, cvi.Spec.VolumeID)
	if err != nil {
		return fmt.Errorf("reconcileIndependentDiskPath: QueryLiveDiskPath failed for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileIndependentDiskPath: QueryLiveDiskPath succeeded for volume %q — diskPath=%q",
		cvi.Spec.VolumeID, livePath)

	specPatch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"diskPath": livePath,
		},
	})
	if err != nil {
		return fmt.Errorf("reconcileIndependentDiskPath: failed to marshal spec patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, specPatch); err != nil {
		return fmt.Errorf("reconcileIndependentDiskPath: failed to patch spec.diskPath for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileIndependentDiskPath: patched spec.diskPath for volume %q", cvi.Spec.VolumeID)
	return nil
}

// reconcileDiskPathRefresh services a vm-operator-requested refresh of
// spec.diskPath (DiskPathRefreshRequestedAnnotation): vm-operator's
// ReconfigVM_Task failed with FileNotFound against the exact path it read,
// meaning the disk was relocated after CSI last resolved it. This is a
// plain re-resolve, not a transfer — no unregister, no ownership change, no
// finalizer — safe for either disk mode. The new value replaces the old one
// directly in one patch, so spec.diskPath is never observably empty in
// between; that matters for a dependent volume, where
// status.ownership==VMManaged is a durable invariant that diskPath is
// non-empty. The annotation is cleared only after that patch succeeds, so a
// crash in between leaves the annotation for a retry rather than losing the
// request.
//
// The live read itself differs by mode. An independent-mode volume is still
// a CNS-registered FCD, so QueryLiveDiskPath's CNS QueryVolumeInfo call
// works. A dependent-mode (VMManaged) volume was already unregistered by
// reconcileUnregister earlier in its life — it is no longer an FCD, so that
// same CNS call permanently fails "object not found". For that case the
// VM's own device backing (looked up by diskUUID, keyed to the single VM in
// spec.vms) is the only remaining source of truth, and it is authoritative
// because vm-operator's ReconfigVM_Task — the only thing that can move this
// disk while it is VM-managed — updates it synchronously.
func (r *Reconciler) reconcileDiskPathRefresh(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo, hasDependent bool) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	var livePath string
	var err error
	if hasDependent {
		if len(cvi.Spec.VMs) == 0 {
			return fmt.Errorf("reconcileDiskPathRefresh: %q is dependent but spec.vms is empty", cvi.Spec.VolumeID)
		}
		vmInstanceUUID := cvi.Spec.VMs[0].VMInstanceUUID
		log.Infof("reconcileDiskPathRefresh: calling QueryDiskPathFromVM for volume %q on VM %q (refresh requested)",
			cvi.Spec.VolumeID, vmInstanceUUID)
		livePath, err = r.volumeManager.QueryDiskPathFromVM(ctx, vmInstanceUUID, cvi.Spec.DiskUUID)
		switch {
		case errors.Is(err, volumes.ErrDiskNotAttachedToVM):
			// The disk is not part of the VM's hardware, so the VM cannot say
			// where it lives. This is the normal case, not an exception:
			// vm-operator requests a refresh from the failure path of its own
			// AttachVolumeDisks, so at the moment the annotation is written
			// the attach has just failed and the disk is by definition not
			// attached. vm-operator then refuses to attach while the
			// annotation is set (it cannot trust spec.diskPath), and we cannot
			// clear the annotation without a path — a mutual wait that never
			// resolves. Erroring out here is what made that deadlock
			// permanent.
			//
			// Keeping the recorded path is also the right answer, not merely
			// the deadlock-breaking one. Only a VM-level operation
			// (ReconfigVM_Task, storage vMotion) relocates a VMDK, and all of
			// them require the disk to be attached; a detached VMDK cannot
			// have moved since the path was recorded, so the existing value is
			// still correct and the refresh is legitimately a no-op. Clear the
			// annotation and let vm-operator proceed.
			if cvi.Spec.DiskPath == "" {
				// Nothing recorded and nothing to read it from: this would
				// publish an empty diskPath for a VMManaged volume, breaking
				// the invariant vm-operator's attach path relies on. Keep the
				// annotation and retry.
				return fmt.Errorf("reconcileDiskPathRefresh: %q has an empty spec.diskPath and its disk is "+
					"not attached to VM %q, so there is no source to resolve it from: %w",
					cvi.Spec.VolumeID, vmInstanceUUID, err)
			}
			log.Infof("reconcileDiskPathRefresh: volume %q is not attached to VM %q, so nothing can have "+
				"relocated it since spec.diskPath=%q was recorded; treating the refresh as a no-op and "+
				"clearing the annotation so the attach can proceed",
				cvi.Spec.VolumeID, vmInstanceUUID, cvi.Spec.DiskPath)
			return r.clearDiskPathRefreshRequested(ctx, cvi)
		case err != nil:
			return fmt.Errorf("reconcileDiskPathRefresh: QueryDiskPathFromVM failed for %q on VM %q: %w",
				cvi.Spec.VolumeID, vmInstanceUUID, err)
		}
	} else {
		log.Infof("reconcileDiskPathRefresh: calling QueryLiveDiskPath for volume %q (refresh requested)",
			cvi.Spec.VolumeID)
		livePath, err = r.volumeManager.QueryLiveDiskPath(ctx, cvi.Spec.VolumeID)
		if err != nil {
			return fmt.Errorf("reconcileDiskPathRefresh: QueryLiveDiskPath failed for %q: %w",
				cvi.Spec.VolumeID, err)
		}
	}
	log.Infof("reconcileDiskPathRefresh: live read succeeded for volume %q — diskPath=%q",
		cvi.Spec.VolumeID, livePath)

	specPatch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"diskPath": livePath,
		},
	})
	if err != nil {
		return fmt.Errorf("reconcileDiskPathRefresh: failed to marshal spec patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, specPatch); err != nil {
		return fmt.Errorf("reconcileDiskPathRefresh: failed to patch spec.diskPath for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("reconcileDiskPathRefresh: patched spec.diskPath=%q for volume %q", livePath, cvi.Spec.VolumeID)

	return r.clearDiskPathRefreshRequested(ctx, cvi)
}

// recordObservedGeneration republishes status with observedGeneration set to
// the current metadata.generation, preserving ownership, phase and the Ready
// condition exactly as they are. It is a no-op when status is already current.
//
// This exists because observedGeneration is a two-party contract, not
// bookkeeping: vm-operator gates its attach on
// observedGeneration >= generation (IsGreenSignal), so a generation this
// controller has seen and deliberately taken no action on must still be
// acknowledged. Every action branch records its own generation as part of the
// status write it already makes; only the idle branches had no such write.
func (r *Reconciler) recordObservedGeneration(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	if cvi.Status.ObservedGeneration >= cvi.Generation {
		return nil
	}
	// Ownership "" is not a settled state — the initial-status branch owns
	// that transition, and claiming to have observed the generation here would
	// race it.
	if cvi.Status.Ownership == "" {
		return nil
	}

	log.Infof("recordObservedGeneration: %q is idle at generation=%d but status reports "+
		"observedGeneration=%d; republishing status so vm-operator's green signal can be satisfied",
		cvi.Spec.VolumeID, cvi.Generation, cvi.Status.ObservedGeneration)

	ready := readyCondition(cvi)
	reason := reasonInitialCSIManaged
	message := ""
	condReady := true
	if ready != nil {
		reason = ready.Reason
		message = ready.Message
		condReady = ready.Status == metav1.ConditionTrue
	}
	patch := buildStatusPatch(cvi.Generation, cvi.Status.Ownership, cvi.Status.Phase,
		cvi.Status.Error, message, reason, condReady)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch); err != nil {
		return fmt.Errorf("recordObservedGeneration: failed to patch status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	return nil
}

// clearDiskPathRefreshRequested removes DiskPathRefreshRequestedAnnotation.
//
// This is the only exit from the refresh state, and every path through
// reconcileDiskPathRefresh must reach it — vm-operator treats the
// annotation's presence as "spec.diskPath cannot be trusted" and will not
// attach the disk while it is set, so an early return that leaves it in place
// stalls the attach indefinitely.
func (r *Reconciler) clearDiskPathRefreshRequested(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	removeAnnotationPatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{"%s":null}}}`,
		csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation))
	if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, removeAnnotationPatch); err != nil {
		return fmt.Errorf("clearDiskPathRefreshRequested: failed to clear %s for %q: %w",
			csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, cvi.Spec.VolumeID, err)
	}
	log.Infof("clearDiskPathRefreshRequested: cleared %s for volume %q",
		csivolumeinfov1alpha1.DiskPathRefreshRequestedAnnotation, cvi.Spec.VolumeID)
	return nil
}

// reconcileRegister re-registers a formerly VM-managed VMDK as a first-class
// disk (FCD) with full Kubernetes metadata, and transitions the CVI to
// CSIManaged ownership. If the volume is fcd-retained, there is nothing to
// register — the FCD was never unregistered — so this short-circuits to
// finishRelease instead.
//
// Steps:
//  1. If fcd-retained, skip straight to finishRelease: no CreateVolume, no
//     folder-URL conversion (the retained FCD is already a normal FCD).
//  2. Otherwise: fetch the PVC and PV referenced by spec.pvcNamespace/pvcName and spec.pvName.
//  3. Reconstruct CNS entity metadata.
//  4. Resolve the SPBM storage policy ID from PV volumeAttributes or StorageClass.
//  5. Convert spec.diskPath to a folder URL and call volumeManager.CreateVolume
//     (re-register); CnsVolumeAlreadyExistsFault → success.
//  6. finishRelease: clear fcd-retained if present, patch status to
//     CSIManaged/Succeeded, remove the volume-protection finalizer.
func (r *Reconciler) reconcileRegister(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	if _, retained := cvi.Annotations[csivolumeinfov1alpha1.FcdRetainedAnnotation]; retained {
		log.Infof("reconcileRegister: volume %q is fcd-retained; the FCD was never unregistered — "+
			"skipping CreateVolume and folder-URL conversion", cvi.Spec.VolumeID)
		return r.finishRelease(ctx, cvi)
	}
	log.Infof("reconcileRegister: starting for volume %q (diskPath=%q)", cvi.Spec.VolumeID, cvi.Spec.DiskPath)

	// Fetch PVC. A missing PVC must not be fatal.
	//
	// The webhook refuses to delete a PVC whose volume is VMManaged, but that
	// is a per-object admission check and namespace teardown does not go
	// through it: WCP/GC deletes the contents of a terminating namespace
	// directly, so the PVC can and does vanish while the CVI is still
	// VMManaged. Once that happens this Get can never succeed again, and
	// because reconcileRegister is the only route to finishRelease — the only
	// place the volume-protection finalizer is removed — treating it as an
	// error stranded the CVI permanently: finalizer held forever, and the PV
	// behind it undeletable via its blockOwnerDeletion ownerRef. An
	// unrecoverable state reachable by an ordinary `kubectl delete namespace`.
	//
	// The PVC is needed for exactly one thing below: pvcMeta's name, namespace
	// and labels. Two of those three are already recorded on the CVI spec, and
	// the labels are CNS metadata for a claim that no longer exists — nothing
	// consumes them for a volume on its way out. So re-register with the
	// recorded identity and no labels, which lets the volume reach CSIManaged,
	// drop its finalizer, and be cleaned up by the normal CSI delete path.
	var pvcName, pvcNamespace string
	var pvcLabels map[string]string
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Namespace: cvi.Spec.PVCNamespace,
		Name:      cvi.Spec.PVCName,
	}, pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("reconcileRegister: failed to get PVC %s/%s: %w",
				cvi.Spec.PVCNamespace, cvi.Spec.PVCName, err)
		}
		log.Infof("reconcileRegister: PVC %s/%s is already gone (namespace teardown bypasses the "+
			"per-PVC admission check); re-registering %q from the identity recorded on the CVI with no "+
			"labels so the volume can still be released",
			cvi.Spec.PVCNamespace, cvi.Spec.PVCName, cvi.Spec.VolumeID)
		pvcName, pvcNamespace = cvi.Spec.PVCName, cvi.Spec.PVCNamespace
	} else {
		pvcName, pvcNamespace, pvcLabels = pvc.Name, pvc.Namespace, pvc.Labels
	}

	// Fetch PV.
	pv := &corev1.PersistentVolume{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: cvi.Spec.PVName}, pv); err != nil {
		return fmt.Errorf("reconcileRegister: failed to get PV %q: %w", cvi.Spec.PVName, err)
	}

	// Resolve cluster ID.
	clusterID := r.configInfo.Cfg.Global.ClusterID
	if clusterID == "" {
		clusterID = r.configInfo.Cfg.Global.SupervisorID
	}

	// Extract vCenter user for containerCluster.
	var vcUser, clusterDist string
	for _, vcCfg := range r.configInfo.Cfg.VirtualCenter {
		vcUser = vcCfg.User
		break
	}
	clusterDist = r.configInfo.Cfg.Global.ClusterDistribution

	containerCluster := cnsvsphere.GetContainerCluster(
		clusterID, vcUser, cnstypes.CnsClusterFlavorWorkload, clusterDist)

	// Reconstruct entity references from the live PVC and PV.
	pvRef := cnsvsphere.CreateCnsKuberenetesEntityReference(
		string(cnstypes.CnsKubernetesEntityTypePV),
		pv.Name, "", clusterID)

	pvcMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pvcName, pvcLabels, false,
		string(cnstypes.CnsKubernetesEntityTypePVC),
		pvcNamespace, clusterID,
		[]cnstypes.CnsKubernetesEntityReference{pvRef})

	pvMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pv.Name, pv.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePV),
		"", clusterID, nil)

	// Resolve the SPBM profile ID.
	storagePolicyID := resolveStoragePolicyID(ctx, r.client, pv)

	// Convert the datastore path captured during unregister to the HTTP folder
	// URL that CNS's RegisterVMDKWithUrlAction requires. BackingDiskId cannot be
	// used here because the FCD entry no longer exists after UnregisterVolumeEx.
	diskFolderURL, err := r.volumeManager.GetDiskFolderURL(ctx, cvi.Spec.DiskPath)
	if err != nil {
		return fmt.Errorf("reconcileRegister: failed to resolve disk folder URL for %q: %w",
			cvi.Spec.DiskPath, err)
	}

	// Preserve the original FCD ID by setting VolumeId on the top-level spec.
	// When FCD_TRANSACTION_SUPPORT is enabled (vSphere 9.2+) CNS passes this to
	// FcdSvc()->RegisterDisk so the re-registered FCD retains the same UUID.
	origVolumeID := cnstypes.CnsVolumeId{Id: cvi.Spec.VolumeID}
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       pv.Name,
		VolumeType: string(cnstypes.CnsVolumeTypeBlock),
		VolumeId:   &origVolumeID,
		Profile:    buildStorageProfileSpec(storagePolicyID),
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        []cnstypes.BaseCnsEntityMetadata{pvcMeta, pvMeta},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			BackingDiskUrlPath: diskFolderURL,
		},
	}

	log.Infof("reconcileRegister: calling CreateVolume for volume %q", cvi.Spec.VolumeID)
	_, faultType, err := r.volumeManager.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		// An already-registered backing disk means the volume is back under CSI
		// management, which is the desired end state — treat it as success. CNS
		// reports this as either CnsAlreadyRegisteredFault (re-register) or
		// CnsVolumeAlreadyExistsFault.
		if volumes.IsCnsAlreadyRegisteredFault(ctx, faultType) ||
			volumes.IsCnsVolumeAlreadyExistsFault(ctx, faultType) {
			log.Infof("reconcileRegister: volume %q already registered as FCD — treating as success",
				cvi.Spec.VolumeID)
		} else {
			return fmt.Errorf("reconcileRegister: CreateVolume failed for %q (fault=%q): %w",
				cvi.Spec.VolumeID, faultType, err)
		}
	} else {
		log.Infof("reconcileRegister: CreateVolume succeeded for volume %q", cvi.Spec.VolumeID)
	}

	return r.finishRelease(ctx, cvi)
}

// clearStrayVolumeProtectionFinalizer removes a VolumeProtectionFinalizer
// that has no remaining reason to exist: no dependent VM is attached, the
// volume is not VMManaged, and no reconcile ever reached UnregisterVolumeEx
// for it. That combination is only reachable one way — reconcileUnregister
// added the finalizer, then the attach was cancelled (vm-operator emptied
// spec.vms) before the reconcile could either make the destructive call or
// land its status flip. Nothing else will ever clean it up: finishRelease is
// only reachable from reconcileRegister, which requires ownership==VMManaged,
// so the volume settles into the initial-CSIManaged or idle branch of the
// decision table with a permanent finalizer, and a permanently undeletable
// PV behind it.
//
// The correctness of this rests entirely on the unregister-attempted
// annotation being a separate bit from the finalizer. Gating on
// "!hasDependent && ownership != VMManaged" alone is NOT safe — a reconcile
// that is at this moment inside UnregisterVolumeEx has both of those
// properties from a cached reader's point of view, and stripping its
// finalizer would erase the evidence its own crash-recovery path depends on,
// letting it converge to VMManaged with nothing blocking GC. With the
// annotation in the gate, the only window where this fires is the window
// where no destructive call has happened or is about to: if a concurrent
// reconcile has written the marker, this returns early, and if it has not
// written the marker it has not called UnregisterVolumeEx either and will
// re-add the finalizer itself when it gets there.
//
// Both reads that decide this must come from the API server, not the cache:
// the whole point is that the cached view of a concurrent reconcile's writes
// may be stale, and a stale "no marker" is exactly the misread that would
// make this unsafe. The write is optimistic-locked on that live read, so a
// concurrent writer wins and this reconcile requeues.
//
// On top of that, the FCD's continued existence is confirmed by a live query
// before the write — see the QueryLiveDiskPath call below for why inference
// from the marker alone is not enough for pre-existing CVIs.
//
// Returns true when the caller should requeue rather than continue.
func (r *Reconciler) clearStrayVolumeProtectionFinalizer(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo, hasDependent bool) (bool, error) {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	// Cheap precondition on the cached object, purely to keep the live read
	// off the common path. Every condition is re-checked below.
	if !hasVolumeProtectionFinalizer(cvi) || hasDependent ||
		cvi.Status.Ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged ||
		hasUnregisterAttempted(cvi) {
		return false, nil
	}

	live := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	if err := r.apiReader.Get(ctx, k8stypes.NamespacedName{
		Namespace: cvi.Namespace,
		Name:      cvi.Name,
	}, live); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("clearStrayVolumeProtectionFinalizer: live read failed for %q: %w",
			cvi.Spec.VolumeID, err)
	}

	if !hasVolumeProtectionFinalizer(live) || hasDependentEntry(ctx, live.Spec.VMs) ||
		live.Status.Ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged ||
		hasUnregisterAttempted(live) {
		log.Infof("clearStrayVolumeProtectionFinalizer: cached state suggested a stray finalizer on %q "+
			"but the live object disagrees (finalizer=%t hasDependent=%t ownership=%q attempted=%t); "+
			"leaving it alone", cvi.Spec.VolumeID, hasVolumeProtectionFinalizer(live),
			hasDependentEntry(ctx, live.Spec.VMs), live.Status.Ownership, hasUnregisterAttempted(live))
		return false, nil
	}

	// Positive confirmation before a destructive-to-invariants write: the FCD
	// must still exist. A successful live query proves the volume was never
	// unregistered, so the finalizer is genuinely stray and removing it cannot
	// unblock GC on a disk that has left CSI's hands.
	//
	// Absence of the marker is nearly, but not quite, sufficient on its own.
	// For a CVI written before the marker existed, "no marker" carries no
	// information: a legacy object stranded with a completed unregister and a
	// failed status flip, whose VM then detached, is indistinguishable from a
	// legacy cancelled attach — and stripping the finalizer in the first case
	// is exactly the VMManaged-with-no-protection outcome this whole change
	// exists to make impossible. Rather than infer, ask.
	//
	// NotFound (or any query error) therefore means leave it alone. That
	// forgoes cleaning up some strays, which is the right way to be wrong
	// here: a leaked finalizer is a visible, manually repairable nuisance,
	// while an unprotected VM-owned disk is silent data risk.
	if _, err := r.volumeManager.QueryLiveDiskPath(ctx, cvi.Spec.VolumeID); err != nil {
		log.Infof("clearStrayVolumeProtectionFinalizer: %q looks like a stray finalizer, but its FCD "+
			"could not be confirmed to still exist (%v); leaving the finalizer in place rather than "+
			"risk unprotecting an already-unregistered disk", cvi.Spec.VolumeID, err)
		return false, nil
	}

	log.Infof("clearStrayVolumeProtectionFinalizer: %q carries a volume-protection finalizer with no "+
		"dependent VM, ownership=%q, no unregister attempt on record, and a still-registered FCD — "+
		"an attach was cancelled before ownership transfer began; removing it",
		cvi.Spec.VolumeID, live.Status.Ownership)
	if err := r.cviSvc.RemoveStrayVolumeProtectionFinalizer(ctx, live); err != nil {
		if apierrors.IsConflict(err) {
			log.Infof("clearStrayVolumeProtectionFinalizer: lost the optimistic lock on %q to a "+
				"concurrent writer; requeueing: %v", cvi.Spec.VolumeID, err)
			return true, nil
		}
		return false, fmt.Errorf("clearStrayVolumeProtectionFinalizer: failed to remove finalizer "+
			"from %q: %w", cvi.Spec.VolumeID, err)
	}
	return false, nil
}

// finishRelease flips the CVI back to CSIManaged/Succeeded and removes the
// volume-protection finalizer — the common tail of both the fcd-retained
// skip branch and a completed re-register, kept as one helper so the two
// paths cannot drift apart. Clears the fcd-retained and unregister-attempted
// annotations first, if present, so status.ownership never reads CSIManaged
// while the annotations still claim the FCD is retained or unregistered.
//
// Clearing unregister-attempted before (not after) the finalizer removal is
// the safe ordering: the intermediate state a concurrent reconcile can
// observe is "no marker, finalizer still present", which is exactly the state
// clearStrayVolumeProtectionFinalizer is there to finish off. The reverse
// order would expose "marker set, no finalizer" — a state that would send
// reconcileUnregister's crash-recovery path to VMManaged with nothing
// blocking GC.
func (r *Reconciler) finishRelease(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	_, retained := cvi.Annotations[csivolumeinfov1alpha1.FcdRetainedAnnotation]
	marked := hasUnregisterAttempted(cvi)
	if retained || marked {
		annotations := map[string]interface{}{}
		if retained {
			annotations[csivolumeinfov1alpha1.FcdRetainedAnnotation] = nil
		}
		if marked {
			annotations[csivolumeinfov1alpha1.UnregisterAttemptedAnnotation] = nil
		}
		annotationPatch, err := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": annotations,
			},
		})
		if err != nil {
			return fmt.Errorf("finishRelease: failed to marshal annotation clear patch for %q: %w",
				cvi.Spec.VolumeID, err)
		}
		if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, annotationPatch); err != nil {
			return fmt.Errorf("finishRelease: failed to clear annotations for %q: %w",
				cvi.Spec.VolumeID, err)
		}
		log.Infof("finishRelease: cleared annotations (fcd-retained=%t unregister-attempted=%t) "+
			"for volume %q", retained, marked, cvi.Spec.VolumeID)
	}

	statusPatch := buildStatusPatch(cvi.Generation,
		csivolumeinfov1alpha1.OwnershipStateCSIManaged,
		csivolumeinfov1alpha1.PhaseSucceeded, "", "", reasonRegisterSucceeded, true)
	if err := r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, statusPatch); err != nil {
		return fmt.Errorf("finishRelease: failed to patch status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("finishRelease: status patched to CSIManaged/Succeeded for volume %q", cvi.Spec.VolumeID)

	if err := r.cviSvc.RemoveVolumeProtectionFinalizer(ctx, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("finishRelease: failed to remove protection finalizer for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("finishRelease: volume-protection finalizer removed for volume %q", cvi.Spec.VolumeID)
	return nil
}

// ensurePVOwnerRef sets a PersistentVolume ownerReference on the CsiVolumeInfo
// if one is not already present. This enables Kubernetes GC to cascade-delete
// the CVI when the PV is deleted (CSIManaged path) and blockOwnerDeletion to
// prevent PV deletion while the volume-protection finalizer is present (VMManaged path).
//
// If the PV does not exist yet (race between CreateVolume and PV creation by the CO),
// the error is returned so controller-runtime requeues the reconcile.
func (r *Reconciler) ensurePVOwnerRef(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	if cvi.Spec.PVName == "" {
		log.Debugf("ensurePVOwnerRef: spec.pvName empty, skipping")
		return nil
	}

	// Already set — nothing to do.
	for _, ref := range cvi.OwnerReferences {
		if ref.Kind == "PersistentVolume" && ref.Name == cvi.Spec.PVName {
			return nil
		}
	}

	pv := &corev1.PersistentVolume{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: cvi.Spec.PVName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			// PV not yet created by the CO; requeue so we retry once it exists.
			return fmt.Errorf("ensurePVOwnerRef: PV %q not found yet for CVI %q, will retry",
				cvi.Spec.PVName, cvi.Name)
		}
		return fmt.Errorf("ensurePVOwnerRef: failed to get PV %q: %w", cvi.Spec.PVName, err)
	}

	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "PersistentVolume",
		Name:               pv.Name,
		UID:                pv.UID,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"ownerReferences": []metav1.OwnerReference{ownerRef},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("ensurePVOwnerRef: failed to marshal patch: %w", err)
	}

	if err := r.client.Patch(ctx, cvi, client.RawPatch(k8stypes.MergePatchType, patchBytes)); err != nil {
		return fmt.Errorf("ensurePVOwnerRef: failed to patch CVI %q: %w", cvi.Name, err)
	}

	log.Infof("ensurePVOwnerRef: set PV ownerRef (pv=%q uid=%q) on CVI %q",
		pv.Name, pv.UID, cvi.Name)
	return nil
}

// setFailedStatus patches status.phase=Failed with an error message and
// sets the Ready condition to False. observedGeneration is also updated.
func (r *Reconciler) setFailedStatus(ctx context.Context,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo, errMsg string) error {
	patch := buildStatusPatch(cvi.Generation,
		cvi.Status.Ownership, // do not change ownership on failure
		csivolumeinfov1alpha1.PhaseFailed,
		errMsg, errMsg, reasonReconcileFailed, false)
	return r.cviSvc.PatchCsiVolumeInfoStatus(ctx, cvi.Spec.VolumeID, patch)
}

// buildStatusPatch constructs a JSON merge-patch for the status subresource.
// It always sets observedGeneration to ensure vm-operator's wait condition is met.
//
// statusError and condMessage are deliberately separate: statusError populates
// status.error and must be empty for anything short of a genuine reconcile
// failure, while condMessage is always safe to set — a deferred fcd-retained
// volume carries the blocker's detail on the condition message without that
// message also being read back as status.error.
func buildStatusPatch(generation int64, ownership csivolumeinfov1alpha1.OwnershipState,
	phase csivolumeinfov1alpha1.PhaseState, statusError, condMessage string,
	condReason string, condReady bool) []byte {

	condStatus := metav1.ConditionFalse
	if condReady {
		condStatus = metav1.ConditionTrue
	}
	cond := map[string]interface{}{
		"type":               conditionTypeReady,
		"status":             string(condStatus),
		"reason":             condReason,
		"message":            condMessage,
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
	}
	statusMap := map[string]interface{}{
		"status": map[string]interface{}{
			"ownership":          string(ownership),
			"phase":              string(phase),
			"observedGeneration": generation,
			"error":              statusError,
			"conditions":         []interface{}{cond},
		},
	}
	b, _ := json.Marshal(statusMap)
	return b
}

// resolveStoragePolicyID extracts the SPBM storage policy ID from the PV.
// It first tries pv.spec.csi.volumeAttributes["storagePolicyID"], then falls
// back to the StorageClass parameters["storagePolicyID"] if not found.
func resolveStoragePolicyID(ctx context.Context, c client.Client, pv *corev1.PersistentVolume) string {
	log := logger.GetLogger(ctx).With("pvName", pv.Name)

	if pv.Spec.CSI != nil {
		if id, ok := pv.Spec.CSI.VolumeAttributes["storagePolicyID"]; ok && id != "" {
			log.Infof("resolveStoragePolicyID: found storagePolicyID %q in PV volumeAttributes", id)
			return id
		}
	}

	// Fall back to the StorageClass parameters.
	scName := pv.Spec.StorageClassName
	if scName == "" {
		log.Warnf("resolveStoragePolicyID: PV has no storageClassName; cannot resolve policy ID")
		return ""
	}

	sc := &storagev1.StorageClass{}
	if err := c.Get(ctx, k8stypes.NamespacedName{Name: scName}, sc); err != nil {
		log.Warnf("resolveStoragePolicyID: failed to get StorageClass %q: %v", scName, err)
		return ""
	}

	if id, ok := sc.Parameters["storagePolicyID"]; ok && id != "" {
		log.Infof("resolveStoragePolicyID: found storagePolicyID %q in StorageClass %q", id, scName)
		return id
	}

	log.Warnf("resolveStoragePolicyID: storagePolicyID not found in PV or StorageClass %q", scName)
	return ""
}

// buildStorageProfileSpec returns the CNS storage profile spec for the given policy ID.
// Returns nil if the policy ID is empty.
func buildStorageProfileSpec(storagePolicyID string) []vim25types.BaseVirtualMachineProfileSpec {
	if storagePolicyID == "" {
		return nil
	}
	return []vim25types.BaseVirtualMachineProfileSpec{
		&vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: storagePolicyID,
		},
	}
}

// getBackoffDuration returns the current backoff for the given name,
// initialising it to 1 second if absent.
func getBackoffDuration(ctx context.Context, name k8stypes.NamespacedName) time.Duration {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	if _, exists := backOffDuration[name]; !exists {
		backOffDuration[name] = time.Second
	}
	return backOffDuration[name]
}

// doubleBackoffDuration doubles the backoff up to MaxBackOffDurationForReconciler.
func doubleBackoffDuration(ctx context.Context, name k8stypes.NamespacedName) {
	d := getBackoffDuration(ctx, name)
	d = min(d*2, cnsoptypes.MaxBackOffDurationForReconciler)
	updateBackoffEntry(ctx, name, d)
}

// updateBackoffEntry sets the backoff for the given name.
func updateBackoffEntry(ctx context.Context, name k8stypes.NamespacedName, duration time.Duration) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	backOffDuration[name] = duration
}

// deleteBackoffEntry removes the backoff entry for the given name.
func deleteBackoffEntry(ctx context.Context, name k8stypes.NamespacedName) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	delete(backOffDuration, name)
}
