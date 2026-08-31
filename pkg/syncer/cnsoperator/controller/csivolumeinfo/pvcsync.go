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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// syncPVCUsedByAndProtection mirrors spec.vms onto the bound PVC: one
// usedby-vm-<vmInstanceUUID> annotation per entry (read by BYOK and the
// CBT-sync loop to tell whether a PVC is attached to a VM), and the
// pvc-volume-protection finalizer while any entry is present. Both are keyed
// on spec.vms being non-empty, not on status.ownership — a re-homed
// independent disk stays CSIManaged for life yet is genuinely attached, so
// gating on ownership would silently drop the signal for it.
//
// Runs on every reconcile pass regardless of which branch fires (transfer,
// register, convergence, or idle) and is a no-op if nothing changed, since
// this is orthogonal to the ownership state machine.
func (r *Reconciler) syncPVCUsedByAndProtection(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx).With("volumeID", cvi.Spec.VolumeID)

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{
		Namespace: cvi.Spec.PVCNamespace,
		Name:      cvi.Spec.PVCName,
	}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("syncPVCUsedByAndProtection: PVC %s/%s not found; nothing to sync",
				cvi.Spec.PVCNamespace, cvi.Spec.PVCName)
			return nil
		}
		return fmt.Errorf("syncPVCUsedByAndProtection: failed to get PVC %s/%s: %w",
			cvi.Spec.PVCNamespace, cvi.Spec.PVCName, err)
	}

	patch := client.MergeFrom(pvc.DeepCopy())
	changed := false

	desired := make(map[string]struct{}, len(cvi.Spec.VMs))
	for _, vm := range cvi.Spec.VMs {
		if vm.VMInstanceUUID == "" {
			log.Warnf("syncPVCUsedByAndProtection: VM %q has no vmInstanceUUID; skipping its usedby-vm annotation",
				vm.VMName)
			continue
		}
		desired[cnsoptypes.UsedByVMAnnotationPrefix+vm.VMInstanceUUID] = struct{}{}
	}

	if len(desired) > 0 && pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	for key := range desired {
		if _, ok := pvc.Annotations[key]; !ok {
			pvc.Annotations[key] = ""
			changed = true
		}
	}
	for key := range pvc.Annotations {
		if !strings.HasPrefix(key, cnsoptypes.UsedByVMAnnotationPrefix) {
			continue
		}
		if _, wanted := desired[key]; !wanted {
			delete(pvc.Annotations, key)
			changed = true
		}
	}

	if len(cvi.Spec.VMs) > 0 {
		if !controllerutil.ContainsFinalizer(pvc, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer) {
			controllerutil.AddFinalizer(pvc, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer)
			changed = true
		}
	} else if controllerutil.RemoveFinalizer(pvc, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer) {
		changed = true
	}

	if !changed {
		return nil
	}
	if err := r.client.Patch(ctx, pvc, patch); err != nil {
		return fmt.Errorf("syncPVCUsedByAndProtection: failed to patch PVC %s/%s: %w",
			pvc.Namespace, pvc.Name, err)
	}
	log.Infof("syncPVCUsedByAndProtection: patched PVC %s/%s (usedby-vm annotations + pvc-volume-protection "+
		"finalizer) for volume %q", pvc.Namespace, pvc.Name, cvi.Spec.VolumeID)
	return nil
}
