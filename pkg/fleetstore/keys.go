package fleetstore

import (
	"fmt"
	"reflect"

	v1alpha1 "github.com/typeid/hyperfleet-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type registerOption func(*kindMeta)

func WithAccountIDExtractor(fn func(client.Object) *string) registerOption {
	return func(m *kindMeta) {
		m.extractAccountID = fn
	}
}

const GlobalNamespace = "_"

type kindMeta struct {
	kind             string
	listKind         string
	gvk              schema.GroupVersionKind
	global           bool
	newObj           func() client.Object
	newList          func() client.ObjectList
	extractAccountID func(client.Object) *string
}

var (
	typeToKind = map[reflect.Type]kindMeta{}
	kindToMeta = map[string]kindMeta{}
)

func init() {
	gv := v1alpha1.SchemeGroupVersion

	register(gv, "Cluster", false,
		func() client.Object { return &v1alpha1.Cluster{} },
		func() client.ObjectList { return &v1alpha1.ClusterList{} },
		WithAccountIDExtractor(func(obj client.Object) *string {
			c := obj.(*v1alpha1.Cluster)
			return &c.Spec.AccountID
		}),
	)
	register(gv, "NodePool", false, func() client.Object { return &v1alpha1.NodePool{} }, func() client.ObjectList { return &v1alpha1.NodePoolList{} })
	register(gv, "Placement", false, func() client.Object { return &v1alpha1.Placement{} }, func() client.ObjectList { return &v1alpha1.PlacementList{} })
	register(gv, "Manifest", false, func() client.Object { return &v1alpha1.Manifest{} }, func() client.ObjectList { return &v1alpha1.ManifestList{} })
	register(gv, "ManagementCluster", true, func() client.Object { return &v1alpha1.ManagementCluster{} }, func() client.ObjectList { return &v1alpha1.ManagementClusterList{} })
}

func register(gv schema.GroupVersion, kind string, global bool, newObj func() client.Object, newList func() client.ObjectList, opts ...registerOption) {
	m := kindMeta{
		kind:     kind,
		listKind: kind + "List",
		gvk:      gv.WithKind(kind),
		global:   global,
		newObj:   newObj,
		newList:  newList,
	}
	for _, o := range opts {
		o(&m)
	}

	obj := newObj()
	v := reflect.ValueOf(obj).Elem()
	if !v.FieldByName("Spec").IsValid() {
		panic(fmt.Sprintf("fleetstore: registered type %T has no Spec field", obj))
	}
	if !v.FieldByName("Status").IsValid() {
		panic(fmt.Sprintf("fleetstore: registered type %T has no Status field", obj))
	}
	list := newList()
	lv := reflect.ValueOf(list).Elem()
	if !lv.FieldByName("Items").IsValid() {
		panic(fmt.Sprintf("fleetstore: registered list type %T has no Items field", list))
	}

	t := reflect.TypeOf(obj)
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

func GVKFor(kind string) (schema.GroupVersionKind, bool) {
	m, ok := kindToMeta[kind]
	return m.gvk, ok
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

func ExtractAccountID(kind string, obj client.Object) *string {
	m, ok := kindToMeta[kind]
	if !ok {
		return nil
	}
	if m.extractAccountID != nil {
		return m.extractAccountID(obj)
	}
	return nil
}
