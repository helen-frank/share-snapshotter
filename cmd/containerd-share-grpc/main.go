package main

import (
	"context"
	"fmt"
	"net"
	"os"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	sharesnapshot "github.com/helen-frank/share-snapshotter"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// args - listenPath root etcdAddr
func main() {
	// Provide a unix address to listen to, this will be the `address`
	// in the `proxy_plugin` configuration.
	// The root will be used to store the snapshots.
	if len(os.Args) < 4 {
		fmt.Printf("invalid args: usage: %s <unix addr> <root>\n", os.Args[0])
		os.Exit(1)
	}

	// Create a gRPC server
	rpc := grpc.NewServer()

	// Configure your custom snapshotter, this example uses the native
	// snapshotter and a root directory. Your custom snapshotter will be
	// much more useful than using a snapshotter which is already included.
	// https://godoc.org/github.com/containerd/containerd/snapshots#Snapshotter

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Args[3],
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	sn, err := sharesnapshot.NewSnapshotter(os.Args[2], rdb)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	// Convert the snapshotter to a gRPC service,
	// example in github.com/containerd/containerd/contrib/snapshotservice
	service := snapshotservice.FromSnapshotter(sn)

	// Register the service with the gRPC server
	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	// Listen and serve
	l, err := net.Listen("unix", os.Args[1])
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	if err := rpc.Serve(l); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}
