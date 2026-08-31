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

package config

import (
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// structuralSchemaForVersion loads the embedded CsiVolumeInfo CRD YAML and
// builds the structural schema for the given version, the same schema the
// real API server uses to prune unknown fields on write. This is what
// actually catches a CRD that fell behind the Go type: a fake client accepts
// and returns any field regardless of the CRD, so it would not notice.
func structuralSchemaForVersion(t *testing.T, version string) *structuralschema.Structural {
	t.Helper()

	raw, err := EmbedCsiVolumeInfoCRFile.ReadFile(EmbedCsiVolumeInfoCRFileName)
	if err != nil {
		t.Fatalf("failed to read embedded CRD: %v", err)
	}

	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatalf("failed to unmarshal embedded CRD: %v", err)
	}

	var schemaV1 *apiextensionsv1.JSONSchemaProps
	for _, v := range crd.Spec.Versions {
		if v.Name == version {
			schemaV1 = v.Schema.OpenAPIV3Schema
			break
		}
	}
	if schemaV1 == nil {
		t.Fatalf("version %q not found in embedded CRD", version)
	}

	internalSchema := &apiextensions.JSONSchemaProps{}
	convertErr := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		schemaV1, internalSchema, nil)
	if convertErr != nil {
		t.Fatalf("failed to convert JSONSchemaProps: %v", convertErr)
	}

	structural, err := structuralschema.NewStructural(internalSchema)
	if err != nil {
		t.Fatalf("failed to build structural schema: %v", err)
	}
	return structural
}

// TestCRDSchemaPreservesVolumeName proves the embedded CRD declares
// spec.vms[*].volumeName: an object carrying it, pruned against the real CRD
// schema exactly as the API server would prune on write, must still have it
// afterward. Catches the C14 failure mode directly: a stale CRD embed that
// silently discards the field, leaving vm-operator's write looking accepted
// while CSI's later detach correlation loses the data it needs.
func TestCRDSchemaPreservesVolumeName(t *testing.T) {
	structural := structuralSchemaForVersion(t, "v1alpha1")

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: "vol-1",
			VMs: []csivolumeinfov1alpha1.VirtualMachineRef{
				{VMName: "vm-1", VolumeName: "disk-1"},
			},
		},
	}

	unstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cvi)
	if err != nil {
		t.Fatalf("failed to convert to unstructured: %v", err)
	}

	pruning.Prune(unstr, structural, true)

	vms, ok := unstr["spec"].(map[string]interface{})["vms"].([]interface{})
	if !ok || len(vms) != 1 {
		t.Fatalf("expected exactly one vms entry after pruning, got %#v", unstr["spec"])
	}
	vm, ok := vms[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected vms[0] to be a map, got %#v", vms[0])
	}
	if got := vm["volumeName"]; got != "disk-1" {
		t.Errorf("volumeName was pruned by the CRD schema: got %v, want %q. "+
			"The embedded CRD is missing spec.vms[*].volumeName.", got, "disk-1")
	}
}

// TestCRDSchemaPrunesTrulyUnknownField is the control: an unrecognized field
// must still be pruned, so the positive assertion above is proof the schema
// declares volumeName specifically, not that pruning is a no-op.
func TestCRDSchemaPrunesTrulyUnknownField(t *testing.T) {
	structural := structuralSchemaForVersion(t, "v1alpha1")

	unstr := map[string]interface{}{
		"spec": map[string]interface{}{
			"volumeID": "vol-1",
			"vms": []interface{}{
				map[string]interface{}{
					"vmName":             "vm-1",
					"totallyMadeUpField": "should-not-survive",
				},
			},
		},
	}

	pruning.Prune(unstr, structural, true)

	vm := unstr["spec"].(map[string]interface{})["vms"].([]interface{})[0].(map[string]interface{})
	if _, present := vm["totallyMadeUpField"]; present {
		t.Errorf("expected an undeclared field to be pruned, but it survived")
	}
}
