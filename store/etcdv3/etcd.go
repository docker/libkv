package etcdv3

import (
	"context"
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"go.etcd.io/etcd/clientv3"
)

const (
	// ETCDV3 backend
	ETCDV3 store.Backend = "etcdv3"
)

// EtcdV3 is the receiver type for the Store interface
type EtcdV3 struct {
	timeout time.Duration
	client  *clientv3.Client
	leaseID clientv3.LeaseID
	done    chan struct{}
}

// Register registers etcd to libkv
func Register() {
	libkv.AddStore(ETCDV3, New)
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &EtcdV3{
		done: make(chan struct{}),
	}

	cfg := clientv3.Config{
		Endpoints: addrs,
	}

	if options != nil {
		s.timeout = options.ConnectionTimeout
		cfg.DialTimeout = options.ConnectionTimeout
		cfg.DialKeepAliveTimeout = options.ConnectionTimeout
		cfg.AutoSyncInterval = 5 * time.Minute
	}

	if options.Username != "" {
		cfg.Username = options.Username
		cfg.Password = options.Password
	}

	if options.TLS != nil {
		cfg.TLS = options.TLS
	}

	if s.timeout == 0 {
		s.timeout = 10 * time.Second
	}

	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	s.client = cli

	return s, nil
}

func (s *EtcdV3)getLeaseId(ttlSeconds int64) (clientv3.LeaseID, error){
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Grant(ctx, ttlSeconds)
	cancel()
	if err != nil {
		s.client.Close()
		return 0, err
	}

	go func() {
		ch, kaerr := s.client.KeepAlive(context.Background(), resp.ID)
		if kaerr != nil {

		}
		for {
			select {
			case <-s.done:
				return
			case <-ch:
			}
		}
	}()

	return resp.ID, nil
}

// Put a value at the specified key
func (s *EtcdV3) Put(key string, value []byte, options *store.WriteOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	if options != nil {
		leaseId, err := s.getLeaseId(int64(options.TTL.Seconds()))
		if err != nil {
			return err
		}
		_, err = s.client.Put(ctx, key, string(value), clientv3.WithLease(leaseId))
		return err
	}
	_, err := s.client.Put(ctx, key, string(value))
	cancel()

	return err
}

// Get a value given its key
func (s *EtcdV3) Get(key string) (*store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	pair := &store.KVPair{
		Key:       key,
		Value:     resp.Kvs[0].Value,
		LastIndex: uint64(resp.Kvs[0].Version),
	}

	return pair, nil
}

// Delete the value at the specified key
func (s *EtcdV3) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	_, err := s.client.Delete(ctx, key)
	cancel()

	return err
}

// Exists verifies if a Key exists in the store
func (s *EtcdV3) Exists(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Get(ctx, key)
	cancel()
	if err != nil {
		return false, err
	}

	return len(resp.Kvs) != 0, nil
}

// Watch for changes on a key
func (s *EtcdV3) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		pair, err := s.Get(key)
		if err != nil {
			return
		}
		watchCh <- pair

		rch := s.client.Watch(context.Background(), key)
		for {
			select {
			case <-s.done:
				return
			case wresp := <-rch:
				for _, event := range wresp.Events {
					watchCh <- &store.KVPair{
						Key:       string(event.Kv.Key),
						Value:     event.Kv.Value,
						LastIndex: uint64(event.Kv.Version),
					}
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on child nodes under
// a given directory
func (s *EtcdV3) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)

		list, err := s.List(directory)
		if err != nil {
			return
		}
		watchCh <- list

		rch := s.client.Watch(context.Background(), directory, clientv3.WithPrefix())
		for {
			select {
			case <-s.done:
				return
			case wresp := <-rch:
				if len(wresp.Events) > 0 {
					var pairs []*store.KVPair
					for _, event := range wresp.Events {
						pairs = append(pairs, &store.KVPair{
							Key:       string(event.Kv.Key),
							Value:     event.Kv.Value,
							LastIndex: uint64(event.Kv.Version),
						})
					}
					watchCh <- pairs
				}

				// error
				if wresp.Err() != nil {
					return
				}
			}
		}
	}()

	return watchCh, nil
}

type etcdLock struct {
	session *concurrency.Session
	mutex   *concurrency.Mutex
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (s *EtcdV3) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("not implemented")
}

// List the content of a given prefix
func (s *EtcdV3) List(directory string) ([]*store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Get(ctx, directory, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	kvpairs := make([]*store.KVPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		pair := &store.KVPair{
			Key:       string(kv.Key),
			Value:     kv.Value,
			LastIndex: uint64(kv.Version),
		}
		kvpairs = append(kvpairs, pair)
	}

	return kvpairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *EtcdV3) DeleteTree(directory string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	_, err := s.client.Delete(ctx, directory, clientv3.WithPrefix())
	cancel()

	return err
}

// AtomicPut CAS operation on a single value.
// Pass previous = nil to create a new key.
func (s *EtcdV3) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)

	var revision int64
	var presp *clientv3.PutResponse
	var txresp *clientv3.TxnResponse
	var err error
	if previous == nil {
		//旧值存在，则不允许修改
		if exist, err := s.Exists(key); err != nil {
			return false, nil, err
		}else if !exist {
			presp, err = s.client.Put(ctx, key, string(value))
			if presp != nil {
				revision = presp.Header.GetRevision()
			}
		}else{
			return false, nil, store.ErrKeyExists
		}

	} else {

		var cmps = []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(key), "=", string(previous.Value)),
			clientv3.Compare(clientv3.Version(key), "=", int64(previous.LastIndex)),
		}
		txresp, err = s.client.Txn(ctx).If(cmps...).
			Then(clientv3.OpPut(key, string(value))).
			Commit()
		if txresp != nil {
			if txresp.Succeeded {
				revision = txresp.Header.GetRevision()
			}else{
				err = errors.New("key's version not matched!")
			}
		}
	}
	cancel()

	if err != nil {
		return false, nil, err
	}

	pair := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: uint64(revision),
	}

	return true, pair, nil
}

// AtomicDelete cas deletes a single value
func (s *EtcdV3) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	var deleted = false
	var err error
	var txresp *clientv3.TxnResponse
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	if previous == nil {
		//缺少版本信息，则不允许删除
		return false, errors.New("key's version info is needed!")
	} else {
		var cmps = []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(key), "=", string(previous.Value)),
			clientv3.Compare(clientv3.Version(key), "=", int64(previous.LastIndex)),
		}
		txresp, err = s.client.Txn(ctx).If(cmps...).
			Then(clientv3.OpDelete(key)).
			Commit()

		deleted = txresp.Succeeded
		if !deleted {
			err = errors.New("conflicts!")
		}
	}
	cancel()

	if err != nil {
		return false, err
	}

	return deleted, nil
}

// Close closes the client connection
func (s *EtcdV3) Close() {
	close(s.done)
	s.client.Close()
}
