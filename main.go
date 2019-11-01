package main

import (
	"context"
	"fmt"

	// "github.com/docker/docker/api/events"
	// "github.com/docker/docker/api/types"
	"github.com/docker/docker/client"

	"github.com/docker/stacks/pkg/fakes"
	"github.com/docker/stacks/pkg/interfaces"
	"github.com/docker/stacks/pkg/reconciler"
)

func main() {
	fmt.Printf("Creating shim...\n")
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
	}

	_, err = cli.Info(context.Background())
	if err != nil {
		fmt.Printf("Error getting info: %v\n", err)
		return
	}

	stackCli := fakes.NewFakeReconcilerClient()

	shim := interfaces.NewBackendAPIClientShim(cli, stackCli)

	// put the shim in the dispatcher
	rec := reconciler.New(shim)

	rec.Run()
}
