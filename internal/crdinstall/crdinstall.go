/*
Copyright 2026.

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

package crdinstall

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

const fieldManager = "hyperfleet-operator"

// Install reads all YAML files from the given filesystem, parses them as
// CustomResourceDefinition objects, and applies each to the target cluster
// using server-side apply. It blocks until every CRD is Established.
func Install(ctx context.Context, cfg *rest.Config, crds fs.FS) error {
	logger := log.FromContext(ctx).WithName("crd-install")

	client, err := apiextensionsv1client.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create apiextensions client: %w", err)
	}

	entries, err := fs.Glob(crds, "*.yaml")
	if err != nil {
		return fmt.Errorf("glob CRD files: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("no CRD YAML files found in embedded filesystem")
	}

	var crdNames []string
	for _, entry := range entries {
		data, err := fs.ReadFile(crds, entry)
		if err != nil {
			return fmt.Errorf("read %s: %w", entry, err)
		}

		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return fmt.Errorf("unmarshal %s: %w", entry, err)
		}

		jsonData, err := yaml.YAMLToJSON(data)
		if err != nil {
			return fmt.Errorf("convert %s to JSON: %w", entry, err)
		}

		logger.Info("Applying CRD", "name", crd.Name)
		_, err = client.CustomResourceDefinitions().Patch(
			ctx, crd.Name, types.ApplyPatchType, jsonData,
			metav1.PatchOptions{
				FieldManager: fieldManager,
				Force:        ptr.To(true),
			},
		)
		if err != nil {
			return fmt.Errorf("apply CRD %s: %w", crd.Name, err)
		}
		crdNames = append(crdNames, crd.Name)
	}

	for _, name := range crdNames {
		if err := waitForEstablished(ctx, client.CustomResourceDefinitions(), name); err != nil {
			return fmt.Errorf("wait for CRD %s to become established: %w", name, err)
		}
	}

	logger.Info("All CRDs installed", "count", len(crdNames))
	return nil
}

func waitForEstablished(
	ctx context.Context,
	client apiextensionsv1client.CustomResourceDefinitionInterface,
	name string,
) error {
	return wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			crd, err := client.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextensionsv1.Established &&
					cond.Status == apiextensionsv1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		},
	)
}
