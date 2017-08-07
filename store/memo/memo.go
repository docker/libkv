package memo

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	kvs "github.com/docker/libkv/store/memo/memo_kvs"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Memo is the receiver type for the Store interface
type Memo struct {
	kvs kvs.KeyValueStoreClient
}

// Register registers memo to libkv
func Register() {
	libkv.AddStore("memo", New)
}

// New creates a new memo client
func New(addrs []string, options *store.Config) (store.Store, error) {
	conn, err := grpc.Dial(addrs[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	kvs := kvs.NewKeyValueStoreClient(conn)
	return &Memo{kvs: kvs}, nil
}

// Get current value at "key".
func (s *Memo) Get(key string) (*store.KVPair, error) {
	res, err := s.kvs.Fetch(context.Background(), &kvs.FetchRequest{Key: key})
	if err != nil {
		if grpc.Code(err) == codes.NotFound {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	return &store.KVPair{
		Key:       key,
		Value:     res.Value,
		LastIndex: 0,
	}, nil
}

// Put value at "key"
func (s *Memo) Put(key string, value []byte, options *store.WriteOptions) error {
	_, err := s.kvs.Upsert(context.Background(),
		&kvs.UpsertRequest{Key: key, Value: value})
	return err
}

// Delete value at "key"
func (s *Memo) Delete(key string) error {
	_, err := s.kvs.Delete(context.Background(), &kvs.DeleteRequest{Key: key})
	if err != nil && grpc.Code(err) == codes.NotFound {
		return store.ErrKeyNotFound
	}
	return err
}

// Exists checks if "key" is present in the store
func (s *Memo) Exists(key string) (bool, error) {
	_, err := s.kvs.Fetch(context.Background(), &kvs.FetchRequest{Key: key})
	if err == nil {
		return true, nil
	}
	if grpc.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

// Watch for changes. Not supported by memo.
func (s *Memo) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree for changes. Not supported by memo.
func (s *Memo) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// NewLock is not supported by memo.
func (s *Memo) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// List keys with given prefix.
func (s *Memo) List(directory string) ([]*store.KVPair, error) {
	keys, err := s.kvs.List(context.Background(),
		&kvs.ListRequest{Prefix: directory, MaxKeys: 1000000000})
	if err != nil {
		return nil, err
	}
	var res []*store.KVPair
	for _, k := range keys.Items {
		kv, err := s.Get(k.Key)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
	}
	if len(res) == 0 {
		return nil, store.ErrKeyNotFound
	}
	return res, nil
}

// DeleteTree deletes all entries with given prefix.
func (s *Memo) DeleteTree(directory string) error {
	keys, err := s.kvs.List(context.Background(),
		&kvs.ListRequest{Prefix: directory, MaxKeys: 1000000000})
	if err != nil {
		return err
	}
	for _, k := range keys.Items {
		s.Delete(k.Key)
	}
	return nil
}

// AtomicPut is not supported by memo.
func (s *Memo) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, store.ErrCallNotSupported
}

// AtomicDelete is not supported by memo.
func (s *Memo) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return false, store.ErrCallNotSupported
}

// Close the connection
func (s *Memo) Close() {
}
