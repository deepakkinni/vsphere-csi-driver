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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// TestSyncPVCUsedByAndProtection_AddOnEntryAppend verifies that a usedby-vm
// annotation and the pvc-volume-protection finalizer are both added the
// first time spec.vms carries an entry with a vmInstanceUUID — independent
// of ownership, since this CVI is CSIManaged (as an independent-only
// attachment would be for its whole life).
func TestSyncPVCUsedByAndProtection_AddOnEntryAppend(t *testing.T) {
	const volID = "vol-pvc-sync-add"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-a", VMInstanceUUID: "uuid-a", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
	}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 1, nil)
	pvc := newTestPVC("test-pvc", "test-ns")

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pvc, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{client: c, scheme: s, cviSvc: newFakeCviService(c)}
	require.NoError(t, r.syncPVCUsedByAndProtection(context.Background(), cvi))

	updated := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(),
		k8stypes.NamespacedName{Namespace: "test-ns", Name: "test-pvc"}, updated))
	assert.Contains(t, updated.Annotations, cnsoptypes.UsedByVMAnnotationPrefix+"uuid-a")
	assert.Contains(t, updated.Finalizers, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer)
}

// TestSyncPVCUsedByAndProtection_RemoveOnEntryRemoval verifies that removing
// one of two VMs from spec.vms removes only that VM's annotation, keeping
// the other, and keeps the finalizer since an entry remains.
func TestSyncPVCUsedByAndProtection_RemoveOnEntryRemoval(t *testing.T) {
	const volID = "vol-pvc-sync-remove-one"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-b", VMInstanceUUID: "uuid-b", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
	}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 1, nil)
	pvc := newTestPVC("test-pvc", "test-ns")
	pvc.Annotations = map[string]string{
		cnsoptypes.UsedByVMAnnotationPrefix + "uuid-a": "",
		cnsoptypes.UsedByVMAnnotationPrefix + "uuid-b": "",
	}
	pvc.Finalizers = []string{csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer}

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pvc, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{client: c, scheme: s, cviSvc: newFakeCviService(c)}
	require.NoError(t, r.syncPVCUsedByAndProtection(context.Background(), cvi))

	updated := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(),
		k8stypes.NamespacedName{Namespace: "test-ns", Name: "test-pvc"}, updated))
	assert.NotContains(t, updated.Annotations, cnsoptypes.UsedByVMAnnotationPrefix+"uuid-a")
	assert.Contains(t, updated.Annotations, cnsoptypes.UsedByVMAnnotationPrefix+"uuid-b")
	assert.Contains(t, updated.Finalizers, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer,
		"the finalizer must stay while any entry remains")
}

// TestSyncPVCUsedByAndProtection_AllRemovedWhenEmpty verifies that emptying
// spec.vms removes every usedby-vm annotation and the finalizer in one pass.
func TestSyncPVCUsedByAndProtection_AllRemovedWhenEmpty(t *testing.T) {
	const volID = "vol-pvc-sync-empty"
	cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged, "", "", 1, nil)
	pvc := newTestPVC("test-pvc", "test-ns")
	pvc.Annotations = map[string]string{
		cnsoptypes.UsedByVMAnnotationPrefix + "uuid-a": "",
	}
	pvc.Finalizers = []string{csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer}

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pvc, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{client: c, scheme: s, cviSvc: newFakeCviService(c)}
	require.NoError(t, r.syncPVCUsedByAndProtection(context.Background(), cvi))

	updated := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(),
		k8stypes.NamespacedName{Namespace: "test-ns", Name: "test-pvc"}, updated))
	assert.NotContains(t, updated.Annotations, cnsoptypes.UsedByVMAnnotationPrefix+"uuid-a")
	assert.NotContains(t, updated.Finalizers, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer)
}

// TestSyncPVCUsedByAndProtection_SkipsEntryWithoutInstanceUUID verifies that
// an entry missing vmInstanceUUID is skipped (no annotation key can be
// derived from it) without erroring, and does not block the finalizer add
// driven by the rest of spec.vms.
func TestSyncPVCUsedByAndProtection_SkipsEntryWithoutInstanceUUID(t *testing.T) {
	const volID = "vol-pvc-sync-no-uuid"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{
		{VMName: "vm-no-uuid", DiskMode: csivolumeinfov1alpha1.DiskModeIndependentPersistent},
	}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 1, nil)
	pvc := newTestPVC("test-pvc", "test-ns")

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, pvc, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{client: c, scheme: s, cviSvc: newFakeCviService(c)}
	require.NoError(t, r.syncPVCUsedByAndProtection(context.Background(), cvi))

	updated := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(),
		k8stypes.NamespacedName{Namespace: "test-ns", Name: "test-pvc"}, updated))
	assert.Contains(t, updated.Finalizers, csivolumeinfov1alpha1.PVCVolumeProtectionFinalizer)
}

// TestSyncPVCUsedByAndProtection_MissingPVCIsNoop verifies that a bound PVC
// that no longer exists is treated as a no-op rather than an error.
func TestSyncPVCUsedByAndProtection_MissingPVCIsNoop(t *testing.T) {
	const volID = "vol-pvc-sync-missing-pvc"
	vms := []csivolumeinfov1alpha1.VirtualMachineRef{{VMName: "vm-a", VMInstanceUUID: "uuid-a"}}
	cvi := newCVI(volID, vms, csivolumeinfov1alpha1.OwnershipStateCSIManaged, "", "", 1, nil)

	s := newScheme(t)
	c := newFakeClient(t, s, []client.Object{cvi, newTestPV("test-pv")}, interceptor.Funcs{})

	r := &Reconciler{client: c, scheme: s, cviSvc: newFakeCviService(c)}
	assert.NoError(t, r.syncPVCUsedByAndProtection(context.Background(), cvi))
}
