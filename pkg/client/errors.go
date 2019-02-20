package client

// Copied directly from github.com/docker/docker/client due to unexported interfaces and to avoid circular imports

import (
	"fmt"

	"github.com/docker/docker/api/types/versions"
	"github.com/pkg/errors"
)

// errConnectionFailed implements an error returned when connection failed.
type errConnectionFailed struct {
	host string
}

// Error returns a string representation of an errConnectionFailed
func (err errConnectionFailed) Error() string {
	if err.host == "" {
		return "Cannot connect to the Docker daemon. Is the docker daemon running on this host?"
	}
	return fmt.Sprintf("Cannot connect to the Docker daemon at %s. Is the docker daemon running?", err.host)
}

// IsErrConnectionFailed returns true if the error is caused by connection failed.
func IsErrConnectionFailed(err error) bool {
	_, ok := errors.Cause(err).(errConnectionFailed)
	return ok
}

// ErrorConnectionFailed returns an error with host in the error message when connection to docker daemon failed.
func ErrorConnectionFailed(host string) error {
	return errConnectionFailed{host: host}
}

type notFound interface {
	error
	NotFound() bool // Is the error a NotFound error
}

// IsErrNotFound returns true if the error is a NotFound error, which is returned
// by the API when some object is not found.
func IsErrNotFound(err error) bool {
	te, ok := err.(notFound)
	return ok && te.NotFound()
}

// unauthorizedError represents an authorization error in a remote registry.
type unauthorizedError struct {
	cause error
}

// Error returns a string representation of an unauthorizedError
func (u unauthorizedError) Error() string {
	return u.cause.Error()
}

// IsErrUnauthorized returns true if the error is caused
// when a remote registry authentication fails
func IsErrUnauthorized(err error) bool {
	_, ok := err.(unauthorizedError)
	return ok
}

type pluginPermissionDenied struct {
	name string
}

func (e pluginPermissionDenied) Error() string {
	return "Permission denied while installing plugin " + e.name
}

// IsErrPluginPermissionDenied returns true if the error is caused
// when a user denies a plugin's permissions
func IsErrPluginPermissionDenied(err error) bool {
	_, ok := err.(pluginPermissionDenied)
	return ok
}

type notImplementedError struct {
	message string
}

func (e notImplementedError) Error() string {
	return e.message
}

func (e notImplementedError) NotImplemented() bool {
	return true
}

// IsErrNotImplemented returns true if the error is a NotImplemented error.
// This is returned by the API when a requested feature has not been
// implemented.
func IsErrNotImplemented(err error) bool {
	te, ok := err.(notImplementedError)
	return ok && te.NotImplemented()
}

// NewVersionError returns an error if the APIVersion required
// if less than the current supported version
func (cli *Client) NewVersionError(APIrequired, feature string) error {
	if cli.settings.Version != "" && versions.LessThan(cli.settings.Version, APIrequired) {
		return fmt.Errorf("%q requires API version %s, but the Docker daemon API version is %s", feature, APIrequired, cli.settings.Version)
	}
	return nil
}
