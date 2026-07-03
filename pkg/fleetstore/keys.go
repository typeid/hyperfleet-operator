package fleetstore

import (
	"fmt"
	"reflect"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const GlobalNamespace = "_"

type kindMeta struct {
	kind     string
	listKind string
	global   bool
	newObj   func() client.Object
	newList  func() client.ObjectList
}

var (
	typeToKind = map[reflect.Type]kindMeta{}
	kindToMeta = map[string]kindMeta{}
)

func init() {
	register("Cluster", false, func() client.Object { return &v1alpha1.Cluster{} }, func() client.ObjectList { return &v1alpha1.ClusterList{} })
	register("NodePool", false, func() client.Object { return &v1alpha1.NodePool{} }, func() client.ObjectList { return &v1alpha1.NodePoolList{} })
	register("Placement", false, func() client.Object { return &v1alpha1.Placement{} }, func() client.ObjectList { return &v1alpha1.PlacementList{} })
	register("Manifest", false, func() client.Object { return &v1alpha1.Manifest{} }, func() client.ObjectList { return &v1alpha1.ManifestList{} })
	register("ManagementCluster", true, func() client.Object { return &v1alpha1.ManagementCluster{} }, func() client.ObjectList { return &v1alpha1.ManagementClusterList{} })
}

func register(kind string, global bool, newObj func() client.Object, newList func() client.ObjectList) {
	m := kindMeta{
		kind:     kind,
		listKind: kind + "List",
		global:   global,
		newObj:   newObj,
		newList:  newList,
	}
	t := reflect.TypeOf(newObj())
	typeToKind[t] = m
	kindToMeta[kind] = m
}

func KindFor(obj client.Object) (string, error) {
	t := reflect.TypeOf(obj)
	m, ok := typeToKind[t]
	if !ok {
		return "", fmt.Errorf("fleetstore: unregistered type %T", obj)
	}
	return m.kind, nil
}

func KindForList(list client.ObjectList) (string, error) {
	t := reflect.TypeOf(list)
	for _, m := range typeToKind {
		if reflect.TypeOf(m.newList()) == t {
			return m.kind, nil
		}
	}
	return "", fmt.Errorf("fleetstore: unregistered list type %T", list)
}

func IsGlobal(kind string) bool {
	m, ok := kindToMeta[kind]
	return ok && m.global
}

func NewObject(kind string) (client.Object, error) {
	m, ok := kindToMeta[kind]
	if !ok {
		return nil, fmt.Errorf("fleetstore: unknown kind %q", kind)
	}
	return m.newObj(), nil
}

func NewObjectList(kind string) (client.ObjectList, error) {
	m, ok := kindToMeta[kind]
	if !ok {
		return nil, fmt.Errorf("fleetstore: unknown kind %q", kind)
	}
	return m.newList(), nil
}

func RegisteredKinds() []string {
	kinds := make([]string, 0, len(kindToMeta))
	for k := range kindToMeta {
		kinds = append(kinds, k)
	}
	return kinds
}
