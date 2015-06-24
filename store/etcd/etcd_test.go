package etcd

import (
	"testing"
	"time"

	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
)

func makeEtcdClient(t *testing.T) store.Store {
	client := "localhost:4001"

	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			EphemeralTTL:      2 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestEtcdConnection(t *testing.T) {
	kv, err := New(
		[]string{"localhost:4001"},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			EphemeralTTL:      2 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("Could not construct kv store: %v", err)
	}

	if err := kv.Put("/foo", []byte("bar"), nil); err != nil {
		t.Fatalf("Could not put: %v", err)
	}

	kv, err = New(
		[]string{"http://localhost:4001"},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			EphemeralTTL:      2 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("Could not construct kv store: %v", err)
	}

	if err := kv.Put("/foo", []byte("bar"), nil); err != nil {
		t.Fatalf("Could not put: %v", err)
	}
}

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	backup := makeEtcdClient(t)

	testutils.RunTestStore(t, kv, backup)
}
