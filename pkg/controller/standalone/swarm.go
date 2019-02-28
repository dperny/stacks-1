package standalone

import (
	"crypto/tls"
	"net"
	"time"

	swarmapi "github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/xnet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// NOTE(dperny): this is the standard location of the control socket. there's
// no guarantee it would be here in an atypical installation.
const swarmSocket = "unix:///var/run/docker/swarm/control.sock"

func newControlClient() (swarmapi.ControlClient, error) {
	opts := []grpc.DialOption{}
	insecureCreds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	opts = append(opts, grpc.WithTransportCredentials(insecureCreds))
	opts = append(opts, grpc.WithDialer(
		func(addr string, timeout time.Duration) (net.Conn, error) {
			return xnet.DialTimeoutLocal(addr, timeout)
		}))
	conn, err := grpc.Dial(swarmSocket, opts...)
	if err != nil {
		return nil, err
	}

	return swarmapi.NewControlClient(conn), nil
}
