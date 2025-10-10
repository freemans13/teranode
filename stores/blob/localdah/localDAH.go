// Package localdah provides Delete-At-Height (DAH) functionality for blob storage.
// This package implements a wrapper around blob stores that adds automatic cleanup
// of expired blobs based on blockchain height. When a blob is stored with a DAH value,
// it will be automatically deleted when the blockchain reaches that height.
//
// The LocalDAH wrapper maintains a local store for DAH metadata and coordinates
// with the underlying blob store to provide seamless DAH functionality. This is
// particularly useful for blockchain applications where data has a natural expiration
// based on block height, such as transaction data that only needs to be retained
// for a certain number of blocks.
//
// Key features:
// - Automatic cleanup of expired blobs based on blockchain height
// - Transparent integration with any blob.Store implementation
// - Efficient metadata storage for DAH values
// - Background cleanup processes to remove expired data
package localdah

import (
	"context"
	"io"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

// blobStore defines the interface that underlying blob storage implementations must satisfy
// to be compatible with LocalDAH wrapper functionality. This interface encompasses all
// standard blob operations plus DAH-specific methods for metadata management.
type blobStore interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error
	SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error
	GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)
	Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error
	Close(ctx context.Context) error
	SetCurrentBlockHeight(height uint32)
}

// LocalDAH implements Delete-At-Height functionality as a wrapper around blob stores.
// It maintains DAH metadata in a separate store and coordinates automatic cleanup
// of expired blobs when the blockchain reaches specified heights.
//
// The LocalDAH wrapper uses two underlying stores:
// - dahStore: Stores DAH metadata and expiration information
// - blobStore: Stores the actual blob data
//
// This separation allows for efficient DAH operations without affecting the
// performance of the main blob storage operations.
type LocalDAH struct {
	logger    ulogger.Logger
	dahStore  blobStore
	blobStore blobStore
	options   *options.Options
}

// New creates a new LocalDAH wrapper that adds Delete-At-Height functionality to blob stores.
//
// Parameters:
//   - logger: Logger instance for DAH operations
//   - dahStore: Store for DAH metadata and expiration tracking
//   - blobStore: Store for actual blob data
//   - opts: Optional store configuration options
//
// Returns:
//   - *LocalDAH: Configured LocalDAH wrapper instance
//   - error: Any error that occurred during initialization
func New(logger ulogger.Logger, dahStore, blobStore blobStore, opts ...options.StoreOption) (*LocalDAH, error) {
	options := options.NewStoreOptions(opts...)

	b := &LocalDAH{
		logger:    logger,
		dahStore:  dahStore,
		blobStore: blobStore,
		options:   options,
	}

	return b, nil
}

func (l *LocalDAH) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	n, resp, err := l.dahStore.Health(ctx, checkLiveness)
	if err != nil || n <= 0 {
		return n, resp, err
	}

	return l.blobStore.Health(ctx, checkLiveness)
}

func (l *LocalDAH) Close(_ context.Context) error {
	return nil
}

func (l *LocalDAH) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.SetFromReader(ctx, key, fileType, reader, opts...)
	}

	// set the value in the blob store
	return l.blobStore.SetFromReader(ctx, key, fileType, reader, opts...)
}

func (l *LocalDAH) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] Set called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.Set(ctx, key, fileType, value, opts...)
	}

	// set the value in the blob store
	return l.blobStore.Set(ctx, key, fileType, value, opts...)
}

func (l *LocalDAH) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] SetDAH called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	if newDAH == 0 {
		// move the file from the DAH store to the blob store
		reader, err := l.dahStore.GetIoReader(ctx, key, fileType, opts...)
		if err != nil {
			if found, _ := l.blobStore.Exists(ctx, key, fileType, opts...); found {
				// already there
				return nil
			}

			return err
		}

		defer reader.Close()

		err = l.blobStore.SetFromReader(ctx, key, fileType, reader, opts...)
		if err == nil {
			err = l.dahStore.Del(ctx, key, fileType, opts...)
		}

		return err
	}

	// we are setting a DAH, if it's already in the DAH store, reset the DAH, if it is not in the DAH store, move it there
	found, _ := l.dahStore.Exists(ctx, key, fileType, opts...)
	if found {
		return l.dahStore.SetDAH(ctx, key, fileType, newDAH, opts...)
	}

	// move the file from the blob store to the DAH store
	reader, err := l.blobStore.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return err
	}

	defer reader.Close()

	if err = l.dahStore.SetFromReader(ctx, key, fileType, reader, options.WithDeleteAt(newDAH)); err != nil {
		return err
	}

	return nil
}

func (l *LocalDAH) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	dah, err := l.dahStore.GetDAH(ctx, key, fileType, opts...)
	if err != nil {
		// couldn't find it in the DAH store, try the blob store
		l.logger.Errorf("LocalDAH.GetDAH miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetDAH(ctx, key, fileType, opts...)
	}

	return dah, nil
}

func (l *LocalDAH) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	ioReader, err := l.dahStore.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			ioReader, err = l.dahStore.GetIoReader(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			l.logger.Errorf("LocalDAH.GetIoReader miss for %s", utils.ReverseAndHexEncodeSlice(key))
			return l.blobStore.GetIoReader(ctx, key, fileType, opts...)
		}
	}

	return ioReader, nil
}

func (l *LocalDAH) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	value, err := l.dahStore.Get(ctx, key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			value, err = l.dahStore.Get(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			l.logger.Errorf("LocalDAH.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
			return l.blobStore.Get(ctx, key, fileType, opts...)
		}
	}

	return value, nil
}

func (l *LocalDAH) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	found, err := l.dahStore.Exists(ctx, key, fileType, opts...)
	if err != nil || !found {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			found, err = l.dahStore.Exists(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			// hash, _ := chainhash.NewHash(key)
			// l.logger.Warnf("LocalDAH.Exists miss for %s", hash.String())
			return l.blobStore.Exists(ctx, key, fileType, opts...)
		}
	}

	return found, nil
}

func (l *LocalDAH) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	_ = l.dahStore.Del(ctx, key, fileType, opts...)
	return l.blobStore.Del(ctx, key, fileType, opts...)
}

func (l *LocalDAH) SetCurrentBlockHeight(height uint32) {
	l.dahStore.SetCurrentBlockHeight(height)
	l.blobStore.SetCurrentBlockHeight(height)
}
