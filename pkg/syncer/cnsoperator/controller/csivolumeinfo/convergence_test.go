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

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

func newSnapshotScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, csivolumeinfov1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, snapv1.AddToScheme(s))
	return s
}

func strPtr(s string) *string { return &s }

// TestMapVolumeSnapshotDeleteToCVI covers the three-hop resolution and its
// early-outs: an unrelated snapshot, a resolvable but non-retained CVI, and
// the happy path.
func TestMapVolumeSnapshotDeleteToCVI(t *testing.T) {
	const volID = "vol-snap-map"

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{VolumeHandle: volID},
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: "test-ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vs", Namespace: "test-ns"},
		Spec: snapv1.VolumeSnapshotSpec{
			Source: snapv1.VolumeSnapshotSource{PersistentVolumeClaimName: strPtr("test-pvc")},
		},
	}

	t.Run("resolves and enqueues a retained CVI", func(t *testing.T) {
		cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged, "", "", 1, nil)
		cvi.Annotations = map[string]string{csivolumeinfov1alpha1.FcdRetainedAnnotation: "true"}

		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, []client.Object{pv, pvc, cvi}, interceptor.Funcs{})

		reqs := mapVolumeSnapshotDeleteToCVI(c)(context.Background(), vs)
		require.Len(t, reqs, 1)
		assert.Equal(t, csivolumeinfov1alpha1.CVINamespace, reqs[0].Namespace)
		assert.Equal(t, cvi.Name, reqs[0].Name)
	})

	t.Run("drops the event when the resolved CVI is not fcd-retained", func(t *testing.T) {
		cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged, "", "", 1, nil)

		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, []client.Object{pv, pvc, cvi}, interceptor.Funcs{})

		reqs := mapVolumeSnapshotDeleteToCVI(c)(context.Background(), vs)
		assert.Empty(t, reqs)
	})

	t.Run("drops the event when the PVC does not exist", func(t *testing.T) {
		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, nil, interceptor.Funcs{})

		reqs := mapVolumeSnapshotDeleteToCVI(c)(context.Background(), vs)
		assert.Empty(t, reqs)
	})

	t.Run("drops the event when the source PVC name is unset", func(t *testing.T) {
		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, nil, interceptor.Funcs{})

		bareVS := &snapv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "bare", Namespace: "test-ns"}}
		reqs := mapVolumeSnapshotDeleteToCVI(c)(context.Background(), bareVS)
		assert.Empty(t, reqs)
	})
}

// TestMapVolumeSnapshotContentDeleteToCVI covers the direct volumeHandle
// resolution path used for VolumeSnapshotContent deletes.
func TestMapVolumeSnapshotContentDeleteToCVI(t *testing.T) {
	const volID = "vol-vsc-map"

	vsc := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vsc"},
		Spec: snapv1.VolumeSnapshotContentSpec{
			Source: snapv1.VolumeSnapshotContentSource{VolumeHandle: strPtr(volID)},
		},
	}

	t.Run("resolves and enqueues a retained CVI", func(t *testing.T) {
		cvi := newCVI(volID, nil, csivolumeinfov1alpha1.OwnershipStateVMManaged, "", "", 1, nil)
		cvi.Annotations = map[string]string{csivolumeinfov1alpha1.FcdRetainedAnnotation: "true"}

		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, []client.Object{cvi}, interceptor.Funcs{})

		reqs := mapVolumeSnapshotContentDeleteToCVI(c)(context.Background(), vsc)
		require.Len(t, reqs, 1)
		assert.Equal(t, cvi.Name, reqs[0].Name)
	})

	t.Run("drops the event when volumeHandle is unset", func(t *testing.T) {
		s := newSnapshotScheme(t)
		c := newFakeClient(t, s, nil, interceptor.Funcs{})

		bareVSC := &snapv1.VolumeSnapshotContent{ObjectMeta: metav1.ObjectMeta{Name: "bare"}}
		reqs := mapVolumeSnapshotContentDeleteToCVI(c)(context.Background(), bareVSC)
		assert.Empty(t, reqs)
	})
}

// TestRecordedBlockerIsLinkedClone_Defensive verifies the parse-defensively
// contract directly: no Ready condition at all must return false (fall
// through to a re-attempt), matching the "safe because the attempt is the
// determination" reasoning.
func TestRecordedBlockerIsLinkedClone_Defensive(t *testing.T) {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	assert.False(t, recordedBlockerIsLinkedClone(cvi))
}
