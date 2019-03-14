package reconciler

import (
	"fmt"
	"reflect"

	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/errdefs"
	"github.com/sirupsen/logrus"

	"github.com/docker/stacks/pkg/interfaces"
	"github.com/docker/stacks/pkg/reconciler/notifier"
)

// Client is the subset of interfaces.BackendClient methods needed to
// implement the Reconciler.
type Client interface {
	// stack methods
	GetSwarmStack(string) (interfaces.SwarmStack, error)

	// service methods
	GetServices(dockerTypes.ServiceListOptions) ([]swarm.Service, error)
	GetService(string, bool) (swarm.Service, error)
	CreateService(swarm.ServiceSpec, string, bool) (*dockerTypes.ServiceCreateResponse, error)
	UpdateService(string, uint64, swarm.ServiceSpec, dockerTypes.ServiceUpdateOptions, bool) (*dockerTypes.ServiceUpdateResponse, error)
	RemoveService(string) error

	// network methods
	GetNetworks(filters.Args) ([]dockerTypes.NetworkResource, error)
	GetNetwork(string) (dockerTypes.NetworkResource, error)
	CreateNetwork(dockerTypes.NetworkCreateRequest) (string, error)
	RemoveNetwork(string) error

	// TODO(dperny): there's a lot more where this came from, but these are the
	// parts we need to make this part go
}

// Reconciler is the interface implemented to do the actual work of computing
// and executing the changes required to bring the cluster's specs in line with
// those defined in the Stack.
type Reconciler interface {
	// Reconcile takes the Kind and ID of an object that may need to be
	// reconciled, and reconciles it. If it is a Stack, it may create new
	// objects and notify that changes have occurred. If the object is a
	// resource, like a service, belonging to a Stack, then it may be updated
	// or deleted to match the stack.
	//
	// Returns an error if the Resource cannot be reconciled, and nil if
	// successful.
	//
	// TODO(dperny): we may actually want to pass a whole
	// (github.com/docker/docker/types/events.Message) object to this, instead
	// of an ID and Kind. That would allow us to optimize our decision on
	// whether or not there is any reconciliation that needs to be done. I've
	// punted on doing so for now for simplicity's sake. We'll optimize later.
	Reconcile(kind, id string) error
}

// reconciler is the object that actually implements the Reconciler interface.
// reconciler is thread-safe, and is synchronous. This means tests for the
// reconciler can be written confined to one goroutine.
type reconciler struct {
	notify notifier.ObjectChangeNotifier
	cli    Client

	// stackResources maps object IDs to the ID of the stack that those objects
	// belong to. it is used to determine if a deleted object belongs to a
	// stack
	stackResources map[string]string
}

// New creates a new Reconciler object, which uses the provided
// ObjectChangeNotifier and Client.
func New(notify notifier.ObjectChangeNotifier, cli Client) Reconciler {
	return newReconciler(notify, cli)
}

// newReconciler creates and returns a reconciler object. This returns the
// raw object, for use internally, instead of the interface as used externally.
func newReconciler(notify notifier.ObjectChangeNotifier, cli Client) *reconciler {
	r := &reconciler{
		notify:         notify,
		cli:            cli,
		stackResources: map[string]string{},
	}
	return r
}

func (r *reconciler) Reconcile(kind, id string) error {
	switch kind {
	case interfaces.StackEventType:
		return r.reconcileStack(id)
	case events.ServiceEventType:
		return r.reconcileService(id)
	default:
		// TODO(dperny): what if it's none of these?
		return nil
	}
}

// reconcileStack implements the ReconcileStack method of the Reconciler
// interface
func (r *reconciler) reconcileStack(id string) error {
	stack, err := r.cli.GetSwarmStack(id)
	switch {
	case errdefs.IsNotFound(err):
		// if the stack isn't found, that means this is actually a deletion
		// event.
		return r.deleteStack(id)
	case err != nil:
		return err
	}

	// we need to create networks before we create services, because services
	// will depend on networks
	for name, network := range stack.Spec.Networks {
		// try getting the network first, to see if it already exists
		existingNw, err := r.cli.GetNetwork(name)
		// not found is good, that means no network yet exists.
		if errdefs.IsNotFound(err) {
			// we don't need the ID of the network
			if _, err := r.cli.CreateNetwork(dockerTypes.NetworkCreateRequest{
				Name:          name,
				NetworkCreate: network,
			}); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// check if the network matches the spec. this is tricky because
			// the spec is type NetworkCreate, but the response is type
			// NetworkResource, and these types are similar but not the same.
			if !networkMatchesSpec(existingNw, network) {
				// try deleting the network and recreating it
				rmErr := r.cli.RemoveNetwork(name)
				if rmErr != nil {
					// return the original error that we got, not the error
					// we got from trying to remove, because the user isn't
					// trying to remove a network, they're trying to create one
					return err
				}

				// if we did remove successfully, recreate the network with the
				// right options
				if _, err := r.cli.CreateNetwork(dockerTypes.NetworkCreateRequest{
					Name:          name,
					NetworkCreate: network,
				}); err != nil {
					return err
				}
			}
		}
	}

	for _, spec := range stack.Spec.Services {
		// try getting the service to see if it already exists
		service, err := r.cli.GetService(spec.Annotations.Name, false)
		// if it doesn't exist create it now
		if errdefs.IsNotFound(err) {
			// TODO(dperny): second 2 arguments?
			// TODO(dperny): we don't cache service data right now, but we
			// might want to do so later
			logrus.Debugf("Unable to find existing service, creating service with spec %+v", spec)
			resp, err := r.cli.CreateService(spec, "", false)
			if err != nil {
				return err
			}
			// when we create the service, add it to the mapping of stack
			// resources. this ensures that if the resource is deleted
			// immediately after, then we still have record of it
			r.stackResources[resp.ID] = id
		} else if err != nil {
			return err
		} else {
			// add the service to the map of resources
			r.stackResources[service.ID] = id
			// if the service already exists, it should be reconciled after
			// this, so notify
			r.notify.Notify("service", service.ID)
		}
	}

	// now that we've verified all services belonging to a stack exist, look
	// for any services that say they belong to a stack but actually don't.
	services, err := r.cli.GetServices(dockerTypes.ServiceListOptions{
		Filters: stackLabelFilter(id),
	})
	if err != nil {
		return err
	}
	for _, service := range services {
		// check if the service belongs to a stack. if the service does not
		// belong to any stack, notify that it needs to be reconciled. If the
		// service for some reason belonged to a different stack entirely, then
		// it would get caught when we reconciled that stack, so we don't need
		// to handle that case here.
		if _, ok := r.stackResources[service.ID]; !ok {
			r.notify.Notify("service", service.ID)
		}
	}

	return nil
}

func (r *reconciler) reconcileService(id string) error {
	// first, of course, we have to actually get the service
	service, err := r.cli.GetService(id, false)
	switch {
	case errdefs.IsNotFound(err):
		// if the service isn't found, that means it has been deleted.
		return r.handleDeletedService(id)
	case err != nil:
		return err
	}

	// now, does the service belong to a stack?
	stackID, ok := service.Spec.Annotations.Labels[interfaces.StackLabel]
	if !ok {
		// if the service does not belong to any stack, then there is no
		// reconciling to be done.
		// TODO(dperny): we may want to cache service IDs mapped to stack IDs
		// so that if someone were to remove the stack label, we could still
		// handle that case, but that's later work
		return nil
	}

	// there is a case that is possible, where the service has its StackLabel
	// changed to a different stack. If this occurs, then the service will be
	// deleted (because it does not belong to the stack it says it does) and
	// then it will be recreated (because the service delete will trigger
	// another pass of reconcileService, which will see that a service
	// belonging to some stack has been deleted, and trigger reconciliation of
	// that stack). we could fix that by checking here against
	// r.stackResources, but testing that is kind of a pain so it has been
	// punted on for this moment.

	// now, get the stack itself.
	// TODO(dperny): we may want to cache stacks so we don't have to do this
	// lookup every time
	stack, err := r.cli.GetSwarmStack(stackID)
	// if the stack has been deleted, then the service must follow with it.
	if errdefs.IsNotFound(err) {
		delete(r.stackResources, id)
		return r.cli.RemoveService(id)
	}
	// any other error means we can't reconcile this service right now
	if err != nil {
		return err
	}

	var (
		expectedSpec swarm.ServiceSpec
		// I don't want to just rely on expectedSpec being the zero value, I
		// would rather affirm through a boolean whether or not a matching spec
		// has been found in the stack specs.
		found bool
	)
	for _, spec := range stack.Spec.Services {
		if spec.Annotations.Name == service.Spec.Annotations.Name {
			expectedSpec = spec
			found = true
			break
		}
	}

	// if there is no matching service spec, then we need to delete the service
	if !found {
		delete(r.stackResources, id)
		return r.cli.RemoveService(id)
	}

	// finally, check if the service is already the same
	// TODO(dperny): is reflect.DeepEqual really the best way to do this?
	if !reflect.DeepEqual(expectedSpec, service.Spec) {
		// the response from UpdateService is irrelevant
		_, err := r.cli.UpdateService(
			id,
			service.Meta.Version.Index,
			expectedSpec,
			dockerTypes.ServiceUpdateOptions{},
			false,
		)
		return err
	}

	// if it is. then there is nothing to do
	return nil
}

func (r *reconciler) deleteStack(id string) error {
	// it doesn't matter if the stack is actually deleted or not, so we don't
	// have to get it from the backend. If it isn't deleted, the services will
	// not be deleted when we reconcile them in a bit.
	//
	// We do have to get all services labeled for this stack
	services, err := r.cli.GetServices(dockerTypes.ServiceListOptions{Filters: stackLabelFilter(id)})
	if err != nil {
		return err
	}
	for _, service := range services {
		r.notify.Notify("service", service.ID)
	}
	return nil
}

func (r *reconciler) handleDeletedService(id string) error {
	stackID, ok := r.stackResources[id]
	if !ok {
		return nil
	}
	// if the service belongs to a stack, but it has been deleted, reconcile
	// the stack. This will either cause the service to be recreated if needed,
	// or nothing will occur if not.
	r.notify.Notify("stack", stackID)
	// delete the mapping, it's done its job
	delete(r.stackResources, id)
	return nil
}

// stackLabelFilter constructs a filter.Args which filters for stacks based on
// the stack label being equal to the stack ID.
func stackLabelFilter(stackID string) filters.Args {
	return filters.NewArgs(
		filters.Arg("label", fmt.Sprintf("%s=%s", interfaces.StackLabel, stackID)),
	)
}

// networkMatchesSpec takes an existing network and a desired NetworkCreate,
// and checks if the existing network fulfills the NetworkCreate
// TODO(dperny): beat the hell out of this function with actual production
// values of as many configurations as possible, because this feels super
// fragile.
func networkMatchesSpec(existing dockerTypes.NetworkResource, new dockerTypes.NetworkCreate) bool {
	// this switch with a bunch of fallthroughs is because it feels cleaner and
	// more readable than a bunch of if statements, or a big boolean expression
	switch {
	case existing.Driver != new.Driver:
		fallthrough
	case existing.Scope != new.Scope:
		fallthrough
	case existing.EnableIPv6 != new.EnableIPv6:
		fallthrough
	case existing.Internal != new.Internal:
		fallthrough
	case existing.Attachable != new.Attachable:
		fallthrough
	case existing.Ingress != new.Ingress:
		fallthrough
	case existing.ConfigOnly != new.ConfigOnly:
		return false
	}

	if new.ConfigFrom != nil && existing.ConfigFrom.Network != new.ConfigFrom.Network {
		return false
	}

	if !reflect.DeepEqual(existing.Options, new.Options) {
		return false
	}

	if !reflect.DeepEqual(existing.Labels, new.Labels) {
		return false
	}

	// the create may not specify an IPAM, but if it does, validate that it
	// matches
	if new.IPAM != nil {
		if existing.IPAM.Driver != new.IPAM.Driver {
			return false
		}
		if !reflect.DeepEqual(existing.IPAM.Options, new.IPAM.Options) {
			return false
		}

		// Ok, so testing equivalency fo IPAM configs is Incredibly Stupid,
		// because if any fields are left empty from an IPAM config in the
		// NetworkCreate, they will be filled in with defaults and be present
		// in the NetworkResource.

		// for starters, if the length of ipam configs in new is 0, then the
		// length of ipam configs in existing should be 1. Anything else is
		// bogus.
		if len(new.IPAM.Config) == 0 && len(existing.IPAM.Config) != 1 {
			return false
		}

		// otherwise, the lengths should be equal
		if len(existing.IPAM.Config) != len(new.IPAM.Config) {
			return false
		}

		// now, checking the contents is a huge pain in the ass, because of
		// defaults. However, IPAMConfig needs at the very least a subnet, so
		// we're going to establish equivalency using subnets as the primary
		// key. additionally, if we iterate through existing configs first,
		// then we can eliminate errors that would result from two identical
		// configs being present, because such a configuration would not be
		// valid for existing
		for _, config := range existing.IPAM.Config {
			// use the variable found to indicate that we found a matching
			// config in the existing spec. if we get all the way through the
			// new config loop and this is still false, there is no matching
			// config.
			found := false
			for _, newConfig := range new.IPAM.Config {
				if config.Subnet == newConfig.Subnet {
					found = true
					// if the newConfig.IPRange was emptystring, we'd expect
					// the existing config's IP range to be a default value
					if newConfig.IPRange != "" && newConfig.IPRange != config.IPRange {
						return false
					}

					if newConfig.Gateway != "" && newConfig.Gateway != config.Gateway {
						return false
					}
					if len(newConfig.AuxAddress) != 0 && !reflect.DeepEqual(newConfig.AuxAddress, config.AuxAddress) {
						return false
					}
					// break out of the new configs loop, we don't need to
					// iterate any further.
					break
				}
			}

			if !found {
				return false
			}
		}
	}

	return true
}
