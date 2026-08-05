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
	"fmt"
	"strings"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// unregisterEligiblePredicate admits an Update when spec changed (generation
// bump) or when the fcd-retained annotation was added or removed. The
// default GenerationChangedPredicate would drop the second case, since an
// annotation-only patch does not bump generation — and that is exactly the
// wake this predicate exists to admit. Scoped narrowly on purpose: a plain
// status write must not re-enter the reconciler in a loop.
var unregisterEligiblePredicate = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		if e.ObjectOld == nil || e.ObjectNew == nil {
			return false
		}
		if e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() {
			return true
		}
		oldRetained := e.ObjectOld.GetAnnotations()[csivolumeinfov1alpha1.FcdRetainedAnnotation]
		newRetained := e.ObjectNew.GetAnnotations()[csivolumeinfov1alpha1.FcdRetainedAnnotation]
		return oldRetained != newRetained
	},
}

// deleteOnlyPredicate admits only Delete events. Used for the VolumeSnapshot
// and VolumeSnapshotContent watches, which exist solely to wake a deferred
// CVI when its blocker clears.
var deleteOnlyPredicate = predicate.Funcs{
	CreateFunc:  func(event.CreateEvent) bool { return false },
	UpdateFunc:  func(event.UpdateEvent) bool { return false },
	DeleteFunc:  func(event.DeleteEvent) bool { return true },
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// snapshotEventMapFunc matches handler.MapFunc; named here so the two
// convergence-watch mapper signatures below stay under the line-length limit.
type snapshotEventMapFunc = func(ctx context.Context, obj client.Object) []reconcile.Request

// mapVolumeSnapshotDeleteToCVI resolves a deleted VolumeSnapshot to the CVI
// of the volume it snapshotted, via three O(1) Gets — no list:
// VolumeSnapshot.spec.source.persistentVolumeClaimName -> PVC.spec.volumeName
// -> PV.spec.csi.volumeHandle -> the deterministic CVI name.
func mapVolumeSnapshotDeleteToCVI(c client.Client) snapshotEventMapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		log := logger.GetLogger(ctx)
		vs, ok := obj.(*snapv1.VolumeSnapshot)
		if !ok || vs.Spec.Source.PersistentVolumeClaimName == nil {
			return nil
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, k8stypes.NamespacedName{
			Namespace: vs.Namespace,
			Name:      *vs.Spec.Source.PersistentVolumeClaimName,
		}, pvc); err != nil {
			log.Infof("mapVolumeSnapshotDeleteToCVI: PVC lookup failed for VolumeSnapshot %s/%s: %v",
				vs.Namespace, vs.Name, err)
			return nil
		}
		if pvc.Spec.VolumeName == "" {
			return nil
		}
		pv := &corev1.PersistentVolume{}
		if err := c.Get(ctx, k8stypes.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			log.Infof("mapVolumeSnapshotDeleteToCVI: PV lookup failed for %q: %v", pvc.Spec.VolumeName, err)
			return nil
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
			return nil
		}
		return enqueueIfFcdRetained(ctx, c, pv.Spec.CSI.VolumeHandle)
	}
}

// mapVolumeSnapshotContentDeleteToCVI resolves a deleted VolumeSnapshotContent
// directly through its spec.source.volumeHandle — one O(1) Get, no PVC/PV hop.
func mapVolumeSnapshotContentDeleteToCVI(c client.Client) snapshotEventMapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		vsc, ok := obj.(*snapv1.VolumeSnapshotContent)
		if !ok || vsc.Spec.Source.VolumeHandle == nil {
			return nil
		}
		return enqueueIfFcdRetained(ctx, c, *vsc.Spec.Source.VolumeHandle)
	}
}

// enqueueIfFcdRetained Gets the deterministic CVI for volumeID and enqueues a
// reconcile only if it exists and carries fcd-retained; otherwise the event
// is dropped rather than triggering a no-op reconcile — at 600,000
// VolumeSnapshots the difference matters.
func enqueueIfFcdRetained(ctx context.Context, c client.Client, volumeID string) []reconcile.Request {
	name := csivolumeinfosvc.GetCsiVolumeInfoCRName(volumeID)
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	if err := c.Get(ctx, k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      name,
	}, cvi); err != nil {
		return nil
	}
	if _, retained := cvi.Annotations[csivolumeinfov1alpha1.FcdRetainedAnnotation]; !retained {
		return nil
	}
	return []reconcile.Request{{NamespacedName: k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      name,
	}}}
}

// recordedBlockerIsLinkedClone reports whether the Ready condition's message
// names LinkedClone as the recorded blocker. Parsed defensively: if the
// condition cannot be found, this returns false so the caller falls through
// to a re-attempt — safe, because the attempt itself is the determination.
func recordedBlockerIsLinkedClone(cvi *csivolumeinfov1alpha1.CsiVolumeInfo) bool {
	for _, c := range cvi.Status.Conditions {
		if c.Type == conditionTypeReady {
			return strings.Contains(c.Message, cnstypes.CnsUnregisterBlockerConditionLinkedClone)
		}
	}
	return false
}

// attemptConvergence re-attempts an in-place unregister for a volume that is
// VMManaged and fcd-retained with at least one VM still attached. Woken by
// the VolumeSnapshot/VolumeSnapshotContent delete watches or a spec change;
// there is no periodic resync.
//
//  1. If the recorded blocker is LinkedClone, short-circuit — a linked clone
//     never converts and is never re-attempted.
//  2. Otherwise run the feasibility query first: the volume already had a
//     blocker, so a query that still reports one saves a doomed unregister
//     and a task record.
//  3. If feasible (or the query itself was inconclusive), re-attempt
//     UnregisterVolumeEx — the attempt is the determination, not the query.
//  4. On success: refresh spec.diskPath/diskUUID from the result, drop
//     fcd-retained. No status transition — ownership is already VMManaged.
//  5. On rejection: re-classify the new fault exactly as a fresh unregister
//     attempt would (handleUnregisterBlocked), so the recorded blocker stays
//     current.
func (r *Reconciler) attemptConvergence(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	if recordedBlockerIsLinkedClone(cvi) {
		log.Infof("attemptConvergence: volume %q is fcd-retained for LinkedClone; never re-attempted",
			cvi.Spec.VolumeID)
		return nil
	}

	log.Infof("attemptConvergence: calling QueryUnregisterFeasibility for volume %q", cvi.Spec.VolumeID)
	feasibilities, err := r.volumeManager.QueryUnregisterFeasibility(ctx, []string{cvi.Spec.VolumeID})
	if err == nil && len(feasibilities) == 1 && feasibilities[0].EvaluationFault == "" && !feasibilities[0].Feasible {
		log.Infof("attemptConvergence: volume %q still reports infeasible; leaving deferred", cvi.Spec.VolumeID)
		return nil
	}

	log.Infof("attemptConvergence: calling UnregisterVolumeEx for volume %q", cvi.Spec.VolumeID)
	backingDiskPath, diskUUID, faultType, unregErr := r.volumeManager.UnregisterVolumeEx(ctx, cvi.Spec.VolumeID)
	if unregErr != nil {
		log.Infof("attemptConvergence: re-attempt still blocked for volume %q (fault=%q): %v",
			cvi.Spec.VolumeID, faultType, unregErr)
		return r.handleUnregisterBlocked(ctx, cvi, cvi.Generation, faultType, unregErr)
	}
	log.Infof("attemptConvergence: UnregisterVolumeEx succeeded for volume %q — diskPath=%q, diskUUID=%q",
		cvi.Spec.VolumeID, backingDiskPath, diskUUID)

	spec := map[string]interface{}{}
	if backingDiskPath != "" {
		spec["diskPath"] = backingDiskPath
	}
	if diskUUID != "" {
		spec["diskUUID"] = diskUUID
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				csivolumeinfov1alpha1.FcdRetainedAnnotation: nil,
			},
		},
	}
	if len(spec) > 0 {
		patch["spec"] = spec
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("attemptConvergence: failed to marshal convergence patch for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	if _, err := r.cviSvc.PatchCsiVolumeInfo(ctx, cvi.Spec.VolumeID, patchBytes); err != nil {
		return fmt.Errorf("attemptConvergence: failed to patch spec/annotations for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("attemptConvergence: fcd-retained annotation cleared for volume %q; no status transition "+
		"(ownership already VMManaged)", cvi.Spec.VolumeID)
	return nil
}
