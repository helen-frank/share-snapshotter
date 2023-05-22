package plugin

import (
	"context"
	"errors"

	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	sharesnapshot "github.com/helen-frank/share-snapshotter"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	// Root directory for the plugin
	RootPath string `toml:"root_path"`

	// etcd addr
	RedisAddr string `toml:"redis_addr"`
}

func init() {
	plugin.Register(&plugin.Registration{
		Type:   plugin.SnapshotPlugin,
		ID:     "fuse-overlayfs",
		Config: &Config{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())

			config, ok := ic.Config.(*Config)
			if !ok {
				return nil, errors.New("invalid fuse-overlayfs configuration")
			}

			rdb := redis.NewClient(&redis.Options{
				Addr:     config.RedisAddr,
				Password: "", // no password set
				DB:       0,  // use default DB
			})

			if _, err := rdb.Ping(context.Background()).Result(); err != nil {
				return nil, err
			}

			root := ic.Root
			if config.RootPath != "" {
				root = config.RootPath
			}

			ic.Meta.Exports["root"] = root

			return sharesnapshot.NewSnapshotter(root, rdb)
		},
	})
}
