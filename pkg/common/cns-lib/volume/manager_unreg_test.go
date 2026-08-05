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

package volume

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
)

// TestMockManagerImplementsInterface verifies that MockManager satisfies the
// full Manager interface (compilation-level check augmented with a runtime
// nil-interface assignment).
func TestMockManagerImplementsInterface(t *testing.T) {
	var _ Manager = MockManager{}
}

// TestMockUnregisterVolumeExSuccess verifies the success path of the mock.
func TestMockUnregisterVolumeExSuccess(t *testing.T) {
	ctx := context.Background()
	m := NewMockManager(false, nil, "")

	backingDiskPath, diskUUID, err := m.UnregisterVolumeEx(ctx, "test-volume-id")

	require.NoError(t, err)
	assert.Empty(t, backingDiskPath)
	assert.Empty(t, diskUUID)
}

// TestMockUnregisterVolumeExFailure verifies the error path of the mock.
func TestMockUnregisterVolumeExFailure(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("cns unregister ex failed")
	m := NewMockManager(true, sentinelErr, "vim25:SystemError")

	_, _, err := m.UnregisterVolumeEx(ctx, "test-volume-id")

	require.Error(t, err)
	assert.Equal(t, sentinelErr, err)
}

// TestMockQueryLiveDiskPathSuccess verifies the success path of the mock.
func TestMockQueryLiveDiskPathSuccess(t *testing.T) {
	ctx := context.Background()
	m := NewMockManager(false, nil, "")

	path, err := m.QueryLiveDiskPath(ctx, "test-volume-id")

	require.NoError(t, err)
	assert.Empty(t, path)
}

// TestMockQueryLiveDiskPathFailure verifies the error path of the mock,
// standing in for a NotFound the live query can return transiently right
// after a storage vMotion to a different datastore.
func TestMockQueryLiveDiskPathFailure(t *testing.T) {
	ctx := context.Background()
	sentinelErr := errors.New("NotFound")
	m := NewMockManager(true, sentinelErr, "vim25:NotFound")

	_, err := m.QueryLiveDiskPath(ctx, "test-volume-id")

	require.Error(t, err)
	assert.Equal(t, sentinelErr, err)
}

// TestDefaultManagerUnregisterVolumeExNilVC verifies that UnregisterVolumeEx
// returns an error immediately when the virtualCenter is nil (no live VC
// required for this unit test path).
func TestDefaultManagerUnregisterVolumeExNilVC(t *testing.T) {
	ctx := context.Background()
	m := &defaultManager{} // virtualCenter is nil

	_, _, err := m.UnregisterVolumeEx(ctx, "vol-001")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "virtual center connection not established")
}

// TestDefaultManagerQueryLiveDiskPathNilVC verifies that QueryLiveDiskPath
// returns an error immediately when the virtualCenter is nil (no live VC
// required for this unit test path).
func TestDefaultManagerQueryLiveDiskPathNilVC(t *testing.T) {
	ctx := context.Background()
	m := &defaultManager{} // virtualCenter is nil

	_, err := m.QueryLiveDiskPath(ctx, "vol-001")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "virtual Center connection not established")
}

// TestUnregisterVolumeResultType verifies that the CnsUnregisterVolumeResult
// type has the expected BackingDiskPath and DiskUUID fields (structural regression
// guard — if the govmomi type changes shape, this test will not compile).
func TestUnregisterVolumeResultType(t *testing.T) {
	r := cnstypes.CnsUnregisterVolumeResult{
		BackingDiskPath: "/vmfs/volumes/ds1/disk.vmdk",
		DiskUUID:        "6000c29a-1234-5678-abcd-ef0123456789",
	}
	assert.Equal(t, "/vmfs/volumes/ds1/disk.vmdk", r.BackingDiskPath)
	assert.Equal(t, "6000c29a-1234-5678-abcd-ef0123456789", r.DiskUUID)
}
