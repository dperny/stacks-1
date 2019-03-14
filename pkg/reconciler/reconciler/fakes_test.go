package reconciler

// this file contains fakes used to test the reconciler

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/errdefs"

	"github.com/docker/stacks/pkg/interfaces"
)

// fakeReconcilerClient is a fake implementing the ReconcilerClient interface,
// which is used to test the reconciler. fakeReconcilerClient only implements
// the strict subset of features needed to make the reconciler go. Most
// notably, it has a half-ass implementation of Filters that only works for
// stack ID labels.
type fakeReconcilerClient struct {
	mu sync.Mutex

	// variable for making IDs. increment this every time we make a new ID.
	// easier to do this than to import github.com/docker/swarmkit/identity
	totallyRandomIDBase int

	// maps id -> stack
	stacks map[string]*interfaces.SwarmStack
	// maps name -> id
	stacksByName map[string]string

	services       map[string]*swarm.Service
	servicesByName map[string]string

	networks       map[string]*dockerTypes.NetworkResource
	networksByName map[string]string
}

// error definitions to reuse
var (
	notFound    = errdefs.NotFound(errors.New("not found"))
	invalidArg  = errdefs.InvalidParameter(errors.New("not valid"))
	unavailable = errdefs.Unavailable(errors.New("not available"))
)

func newFakeReconcilerClient() *fakeReconcilerClient {
	return &fakeReconcilerClient{
		stacks:         map[string]*interfaces.SwarmStack{},
		stacksByName:   map[string]string{},
		services:       map[string]*swarm.Service{},
		servicesByName: map[string]string{},
		networks:       map[string]*dockerTypes.NetworkResource{},
		networksByName: map[string]string{},
	}
}

// GetSwarmStack gets a SwarmStack
func (f *fakeReconcilerClient) GetSwarmStack(idOrName string) (interfaces.SwarmStack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.stacksByName, idOrName)

	stack, ok := f.stacks[id]
	if !ok {
		return interfaces.SwarmStack{}, notFound
	}

	if err := causeAnError("get", stack.Spec.Annotations.Labels); err != nil {
		return interfaces.SwarmStack{}, err
	}
	return *stack, nil
}

// GetServices implements the GetServices method of the BackendClient,
// returning a list of services. It only supports 1 kind of filter, which is
// a filter for stack ID.
func (f *fakeReconcilerClient) GetServices(opts dockerTypes.ServiceListOptions) ([]swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var (
		stackID   string
		hasFilter bool
	)
	// before doing anything, check if there is a filter and it's in the
	// correct form. This lets us error out early if it's not
	if opts.Filters.Len() != 0 {
		var ok bool
		stackID, ok = getStackIDFromLabelFilter(opts.Filters)
		if !ok {
			return nil, invalidArg
		}
		hasFilter = true
	}

	services := []swarm.Service{}

	for _, service := range f.services {
		// if we're filtering on stack ID, and this service doesn't match, then
		// we should skip this service
		if hasFilter && service.Spec.Annotations.Labels[interfaces.StackLabel] != stackID {
			continue
		}
		// otherwise, we should append this service to the set
		services = append(services, *service)
	}

	return services, nil
}

// GetService gets a swarm service
func (f *fakeReconcilerClient) GetService(idOrName string, _ bool) (swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.servicesByName, idOrName)

	service, ok := f.services[id]
	if !ok {
		return swarm.Service{}, notFound
	}

	if err := causeAnError("get", service.Spec.Annotations.Labels); err != nil {
		return swarm.Service{}, unavailable
	}
	return *service, nil
}

// CreateService creates a swarm service.
func (f *fakeReconcilerClient) CreateService(spec swarm.ServiceSpec, _ string, _ bool) (*dockerTypes.ServiceCreateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := causeAnError("create", spec.Annotations.Labels); err != nil {
		return nil, err
	}

	if _, ok := f.servicesByName[spec.Annotations.Name]; ok {
		return nil, invalidArg
	}

	// otherwise, create a service object
	service := &swarm.Service{
		ID: f.newID("service"),
		Meta: swarm.Meta{
			Version: swarm.Version{
				Index: uint64(1),
			},
		},
		Spec: spec,
	}

	f.servicesByName[spec.Annotations.Name] = service.ID
	f.services[service.ID] = service

	return &dockerTypes.ServiceCreateResponse{
		ID: service.ID,
	}, nil
}

// UpdateService updates the service to the provided spec.
func (f *fakeReconcilerClient) UpdateService(
	idOrName string,
	version uint64,
	spec swarm.ServiceSpec,
	_ dockerTypes.ServiceUpdateOptions,
	_ bool,
) (*dockerTypes.ServiceUpdateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.servicesByName, idOrName)
	service, ok := f.services[id]
	if !ok {
		return nil, notFound
	}

	if version != service.Meta.Version.Index {
		return nil, invalidArg
	}

	service.Spec = spec
	service.Meta.Version.Index = service.Meta.Version.Index + 1
	return &dockerTypes.ServiceUpdateResponse{}, nil
}

func (f *fakeReconcilerClient) RemoveService(idOrName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.servicesByName, idOrName)

	service, ok := f.services[id]
	if !ok {
		return notFound
	}

	if err := causeAnError("remove", service.Spec.Annotations.Labels); err != nil {
		return err
	}

	delete(f.services, service.ID)
	delete(f.servicesByName, service.Spec.Annotations.Name)

	return nil
}

// GetNetworks returns all networks. It accepts one kind of filter, which is a
// filter for stack ID. See the doc comment for GetServices.
func (f *fakeReconcilerClient) GetNetworks(filter filters.Args) ([]dockerTypes.NetworkResource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var (
		stackID   string
		hasFilter bool
	)

	if filter.Len() != 0 {
		var ok bool
		stackID, ok = getStackIDFromLabelFilter(filter)
		if !ok {
			return nil, invalidArg
		}
		hasFilter = true
	}

	networks := []dockerTypes.NetworkResource{}

	for _, nw := range f.networks {
		if hasFilter && nw.Labels[interfaces.StackLabel] != stackID {
			continue
		}
		networks = append(networks, *nw)
	}
	return networks, nil
}

// CreateNetwork creates a network from the given request
func (f *fakeReconcilerClient) CreateNetwork(nc dockerTypes.NetworkCreateRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := causeAnError("create", nc.NetworkCreate.Labels); err != nil {
		return "", err
	}

	if _, ok := f.networksByName[nc.Name]; ok {
		return "", invalidArg
	}

	nw := &dockerTypes.NetworkResource{
		Name:   nc.Name,
		ID:     f.newID("network"),
		Scope:  nc.NetworkCreate.Scope,
		Driver: nc.NetworkCreate.Driver,
		Labels: nc.NetworkCreate.Labels,
	}

	f.networks[nw.ID] = nw
	f.networksByName[nw.Name] = nw.ID

	return nw.ID, nil
}

// GetNetwork returns a network with the given name or ID
func (f *fakeReconcilerClient) GetNetwork(name string) (dockerTypes.NetworkResource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.networksByName, name)

	nw, ok := f.networks[id]

	if !ok {
		return dockerTypes.NetworkResource{}, notFound
	}

	if err := causeAnError("get", nw.Labels); err != nil {
		return dockerTypes.NetworkResource{}, err
	}

	return *nw, nil
}

// RemoveNetwork removes a network with the given name or ID
func (f *fakeReconcilerClient) RemoveNetwork(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := resolveID(f.networksByName, name)

	nw, ok := f.networks[id]

	if !ok {
		return notFound
	}

	if err := causeAnError("remove", nw.Labels); err != nil {
		return err
	}

	delete(f.networksByName, nw.Name)
	delete(f.networks, nw.ID)

	return nil
}

// resolveID takes a value that might be an ID or and figures out which it is,
// returning the ID
func resolveID(namesToIds map[string]string, key string) string {
	id, ok := namesToIds[key]
	if !ok {
		return key
	}
	return id
}

// getStackIDFromLabelFilter takes a filters.Args and determines if it includes
// a filter for StackLabel. If so, it returns the Stack ID specified by the
// label and true. If not, it returns emptystring and false.
func getStackIDFromLabelFilter(args filters.Args) (string, bool) {
	labelfilters := args.Get("label")
	// there should only be 1 string here, anything else is not supported
	if len(labelfilters) != 1 {
		return "", false
	}

	// we now have a filter that is in one of two forms:
	// SomeKey or SomeKey=SomeValue
	// We split on the =. If we get 1 string back, it means there is no =, and
	// therefore no value specified for the label.
	kvPair := strings.SplitN(labelfilters[0], "=", 2)
	if len(kvPair) != 2 {
		return "", false
	}

	// make sure the key is StackLabel
	if kvPair[0] != interfaces.StackLabel {
		return "", false
	}

	// don't return true if the value is emptystring. there's no reason
	// emptystring wouldn't be a valid, except that i'm pretty sure allowing it
	// to be a valid ID in this context would invite bugs.
	if kvPair[1] == "" {
		return "", false
	}

	return kvPair[1], true
}

func (f *fakeReconcilerClient) newID(objType string) string {
	index := f.totallyRandomIDBase
	f.totallyRandomIDBase++
	return fmt.Sprintf("id_%s_%v", objType, index)
}

// causeAnError is a helper function to cause errors based on object labels. in
// testing, we may want to simulate a wide variety of error conditions on an
// object. however, because this fake is so simple, errors like a timeout or a
// race are hard to elicit in a straightforward way. in order to make that
// easier, we are going to set a special "makemefail" label in the test, which
// the reconciler production code doesn't care about, but which will instruct
// the fakeReconcilerClient to fail in a specific way.
//
// the function takes 2 args: the operation type (create, update, or remove)
// and the labels of the object, and returns an error if one is desired.
func causeAnError(operation string, labels map[string]string) error {
	failure, ok := labels["makemefail"]
	if !ok {
		return nil
	}

	switch failure {
	case "unavailable":
		// unavailable simulates an error where the client is unavailable for
		// some reason
		return unavailable
	case "invalidarg":
		// invalidarg simulates an error where some part of the spec is invalid
		return invalidArg
	}

	return nil
}
