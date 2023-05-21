package main

import (
	"errors"
	"os"
	"time"

	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"go.etcd.io/etcd/clientv3"
)

var EtcdClient *clientv3.Client

func Init() {
	var err error
	EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{os.Getenv("ETCD_ADDR")},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
}

// Config represents configuration for the native plugin.
type Config struct {
	// Root directory for the plugin
	RootPath string `toml:"root_path"`
}

func main() {
	plugin.Register(&plugin.Registration{
		Type:   plugin.SnapshotPlugin,
		ID:     "share",
		Config: &Config{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())

			config, ok := ic.Config.(*Config)
			if !ok {
				return nil, errors.New("invalid share configuration")
			}

			root := ic.Root
			if len(config.RootPath) != 0 {
				root = config.RootPath
			}

			return NewSnapshotter(root)
		},
	})
}
