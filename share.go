package sharesnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/filters"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/redis/go-redis/v9"
)

const (
	SnapID    = "SNAP_ID"
	SnapUsage = "SNAP_USAGE"
	mountType = "bind"
)

var defaultMountOptions = []string{"rbind"}

type Snapshotter struct {
	root   string
	client *redis.Client
}

func NewSnapshotter(root string, redisClient *redis.Client) (snapshots.Snapshotter, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &Snapshotter{
		root:   root,
		client: redisClient,
	}, nil
}

func (o *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	return o.getInfo(ctx, key)
}

func (o *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	result, err := o.client.Set(ctx, info.Name, info, 0).Result()
	if err != nil {
		return snapshots.Info{}, err
	}

	err = json.Unmarshal([]byte(result), &newInfo)
	return newInfo, err
}

func (o *Snapshotter) Usage(ctx context.Context, key string) (usage snapshots.Usage, err error) {
	info, err := o.getInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		if id, ok := info.Labels[SnapID]; !ok {
			return snapshots.Usage{}, errors.New(SnapID + " not found")
		} else {
			du, err := fs.DiskUsage(ctx, o.getSnapshotDir(id))
			if err != nil {
				return snapshots.Usage{}, err
			}
			return snapshots.Usage(du), nil
		}
	}

	if usageStr, ok := info.Labels[SnapUsage]; !ok {
		return snapshots.Usage{}, errors.New(SnapUsage + " not found")
	} else {
		if err := json.Unmarshal([]byte(usageStr), &usage); err != nil {
			return snapshots.Usage{}, err
		}
		return usage, nil
	}
}

// Prepare creates a new snapshot with the given parent and writable layer.
func (o *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// View returns a readonly view of the given snapshot.
func (o *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

func (o *Snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	info, err := o.getInfo(ctx, key)
	if err != nil {
		return nil, err
	}
	id, ok := info.Labels[SnapID]
	if !ok {
		return nil, errors.New(SnapID + " not found")
	}

	parentInfo, err := o.getInfo(ctx, info.Parent)
	if err != nil {
		return nil, err
	}
	parentid, ok := parentInfo.Labels[SnapID]
	if !ok {
		return nil, errors.New(SnapID + " not found")
	}

	return o.mounts(storage.Snapshot{
		Kind:      info.Kind,
		ID:        id,
		ParentIDs: []string{parentid},
	}), nil
}

// Commit commits the given snapshot to the backend.
func (o *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	info, err := o.getInfo(ctx, key)
	if err != nil {
		return err
	}
	id, ok := info.Labels[SnapID]
	if !ok {
		return errors.New(SnapID + " not found")
	}

	usage, err := fs.DiskUsage(ctx, o.getSnapshotDir(id))
	if err != nil {
		return err
	}

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return fmt.Errorf("failed to commit snapshot: %w", err)
	}
	return nil
}

// Remove removes the given snapshot from the backend.
func (o *Snapshotter) Remove(ctx context.Context, key string) (err error) {
	var (
		renamed, path string
		restore       bool
	)

	err = func() error {
		result, err := o.client.GetDel(ctx, key).Result()
		if err != nil {
			return err
		}

		var info snapshots.Info
		if err := json.Unmarshal([]byte(result), &info); err != nil {
			return err
		}

		id, ok := info.Labels[SnapID]
		if !ok {
			return errors.New(SnapID + " not found")
		}

		path = o.getSnapshotDir(id)
		renamed = filepath.Join(o.root, "snapshots", "rm-"+id)
		if err = os.Rename(path, renamed); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to rename: %w", err)
			}
			renamed = ""
		}

		restore = true
		return nil
	}()
	if err != nil {
		if renamed != "" && restore {
			if err1 := os.Rename(renamed, path); err1 != nil {
				// May cause inconsistent data on disk
				log.G(ctx).WithError(err1).WithField("path", renamed).Error("failed to rename after failed commit")
			}
		}
		return err
	}
	if renamed != "" {
		if err := os.RemoveAll(renamed); err != nil {
			// Must be cleaned up, any "rm-*" could be removed if no active transactions
			log.G(ctx).WithError(err).WithField("path", renamed).Warnf("failed to remove root filesystem")
		}
	}

	return nil
}

func (o *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return err
	}

	results, err := o.client.Keys(ctx, "*").Result()
	if err != nil {
		return err
	}

	for i := range results {
		var info snapshots.Info
		if err := json.Unmarshal([]byte(results[i]), &info); err != nil {
			return err
		}

		if !filter.Match(adaptSnapshot(info)) {
			continue
		}
		return fn(ctx, info)
	}

	return nil
}

func (o *Snapshotter) Close() error {
	return nil
}

func (o *Snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *Snapshotter) getSnapshotDir(id string) string {
	return filepath.Join(o.root, "snapshots", id)
}

func (o *Snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var path, td string

	if kind == snapshots.KindActive || parent == "" {
		td, err = os.MkdirTemp(filepath.Join(o.root, "snapshots"), "new-")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
		if err := os.Chmod(td, 0755); err != nil {
			return nil, fmt.Errorf("failed to chmod %s to 0755: %w", td, err)
		}
		defer func() {
			if err != nil {
				if td != "" {
					if err1 := os.RemoveAll(td); err1 != nil {
						err = fmt.Errorf("remove failed: %v: %w", err1, err)
					}
				}
				if path != "" {
					if err1 := os.RemoveAll(path); err1 != nil {
						err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
					}
				}
			}
		}()
	}

	switch kind {
	case snapshots.KindActive, snapshots.KindView:
	default:
		return nil, fmt.Errorf("snapshot type %v invalid; only snapshots of type Active or View can be created: %w", kind, errdefs.ErrInvalidArgument)
	}

	var base snapshots.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return nil, err
		}
	}

	var parentID string
	if parent != "" {
		parentInfo, err := o.getInfo(ctx, parent)
		if err != nil {
			return nil, err
		}

		if parentInfo.Kind != snapshots.KindCommitted {
			return nil, fmt.Errorf("parent %q is not committed snapshot: %w", parent, errdefs.ErrInvalidArgument)
		}

		if id, ok := parentInfo.Labels[SnapID]; !ok {
			return nil, errors.New(SnapID + " not found")
		} else {
			parentID = id
		}
	}

	count, err := o.client.DBSize(ctx).Result()
	if err != nil {
		return nil, err
	}
	t := time.Now().UTC()

	des, err := os.ReadDir(filepath.Join(o.root, "snapshots"))
	if err != nil {
		return nil, err
	}

	// Id is key plus the number of current folders
	id := strconv.Itoa(int(count) + len(des))
	base.Labels[SnapID] = id
	si := snapshots.Info{
		Parent:  parent,
		Kind:    kind,
		Labels:  base.Labels,
		Created: t,
		Updated: t,
	}

	if err := o.client.Set(ctx, key, si, 0).Err(); err != nil {
		return nil, err
	}

	if parentID != "" {
		xattrErrorHandler := func(dst, src, xattrKey string, copyErr error) error {
			// security.* xattr cannot be copied in most cases (moby/buildkit#1189)
			log.G(ctx).WithError(copyErr).Debugf("failed to copy xattr %q", xattrKey)
			return nil
		}

		copyDirOpts := []fs.CopyDirOpt{
			fs.WithXAttrErrorHandler(xattrErrorHandler),
		}

		parentPath := o.getSnapshotDir(parentID)
		if err = fs.CopyDir(td, parentPath, copyDirOpts...); err != nil {
			return nil, fmt.Errorf("copying of parent failed: %w", err)
		}

		path = o.getSnapshotDir(id)
		if err = os.Rename(td, path); err != nil {
			return nil, fmt.Errorf("failed to rename: %w", err)
		}
		td = ""
	}

	return o.mounts(storage.Snapshot{
		Kind:      kind,
		ID:        id,
		ParentIDs: []string{parentID},
	}), nil
}

func (o *Snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	var (
		roFlag string
		source string
	)

	if s.Kind == snapshots.KindView {
		roFlag = "ro"
	} else {
		roFlag = "rw"
	}

	if len(s.ParentIDs) == 0 || s.Kind == snapshots.KindActive {
		source = o.getSnapshotDir(s.ID)
	} else {
		source = o.getSnapshotDir(s.ParentIDs[0])
	}

	return []mount.Mount{
		{
			Source:  source,
			Type:    mountType,
			Options: append(defaultMountOptions, roFlag),
		},
	}
}

func (o *Snapshotter) getInfo(ctx context.Context, key string) (info snapshots.Info, err error) {
	err = o.client.Get(ctx, key).Scan(info)
	return
}

func adaptSnapshot(info snapshots.Info) filters.Adaptor {
	return filters.AdapterFunc(func(fieldpath []string) (string, bool) {
		if len(fieldpath) == 0 {
			return "", false
		}

		switch fieldpath[0] {
		case "kind":
			switch info.Kind {
			case snapshots.KindActive:
				return "active", true
			case snapshots.KindView:
				return "view", true
			case snapshots.KindCommitted:
				return "committed", true
			}
		case "name":
			return info.Name, true
		case "parent":
			return info.Parent, true
		case "labels":
			if len(info.Labels) == 0 {
				return "", false
			}

			v, ok := info.Labels[strings.Join(fieldpath[1:], ".")]
			return v, ok
		}

		return "", false
	})
}
