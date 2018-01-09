package mysql

import (
	"testing"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	testEndpoint = "localhost:3306"
	testUser     = "root"
	testPassword = ""
	testDatabase = "libkv"
	testTable    = "libkv"
)

func init() {
	DefaultWatchWaitTime = 100 * time.Millisecond
}

func makeMySQLClient(t *testing.T) store.Store {
	kv, err := New([]string{testEndpoint}, &store.Config{
		Username: testUser,
		Password: testPassword,
		Database: testDatabase,
		Table:    testTable,
	})

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()
	ss, err := libkv.NewStore(store.MYSQL, []string{testEndpoint}, &store.Config{
		Username: testUser,
		Password: testPassword,
		Database: testDatabase,
		Table:    testTable,
	})
	assert.NoError(t, err)
	assert.NotNil(t, ss)

	if _, ok := ss.(*MySQL); !ok {
		t.Fatal("Error registering and initializing mysql")
	}

	ss.Close()
}

func TestMySQLStore(t *testing.T) {
	kv := makeMySQLClient(t)
	defer kv.Close()
	lockKV := makeMySQLClient(t)
	defer lockKV.Close()

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunCleanup(t, kv)
}

func TestMySQLStoreExtra(t *testing.T) {
	kv := makeMySQLClient(t)
	defer kv.Close()

	ss, err := New([]string{"xx", "yy"}, nil)
	assert.Equal(t, ErrInvalidCountEndpoint, err)
	assert.Nil(t, ss)

	ok, err := kv.AtomicDelete("a/b/c", nil)
	assert.Equal(t, store.ErrPreviousNotSpecified, err)
	assert.False(t, ok)

	testWatchNonExistKey(t, kv)
	testWatchTreeNonExistKey(t, kv)
}

func testWatchNonExistKey(t *testing.T, kv store.Store) {
	nonexist := "test/watch/nonexist"
	value0 := []byte("hello world")
	value1 := []byte("hello world!!!")
	stopCh := make(chan struct{})

	watchCh, err := kv.Watch(nonexist, stopCh)
	if !assert.NoError(t, err) {
		return
	}

	go func() {
		for i := 0; i < 3; i++ {
			var err error
			time.Sleep(250 * time.Millisecond)
			switch i {
			case 0:
				err = kv.Put(nonexist, value0, nil)
			case 1:
				err = kv.Put(nonexist, value1, nil)
			case 2:
				err = kv.Delete(nonexist)
			}
			assert.NoError(t, err)
		}
	}()

	eventCount := 0
	for {
		select {
		case pair := <-watchCh:
			switch eventCount {
			case 0:
				assert.Nil(t, pair, "first must be nil, because of we watching a non-exit key")
			case 1:
				if assert.NotNil(t, pair) {
					assert.Equal(t, value0, pair.Value)
				}
			case 2:
				if assert.NotNil(t, pair) {
					assert.Equal(t, value1, pair.Value)
				}
			case 3:
				assert.Nil(t, pair, "last must be nil, because of the key has been deleted")
				close(stopCh)
				return
			}
			eventCount++
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
		}
	}
}

func testWatchTreeNonExistKey(t *testing.T, kv store.Store) {
	nonexist := "test/watchtree/nonexist"
	key := "test/watchtree/nonexist/testkey"
	value0 := []byte("hello world")
	value1 := []byte("hello world!!!")
	stopCh := make(chan struct{})

	watchCh, err := kv.WatchTree(nonexist, stopCh)
	if !assert.NoError(t, err) {
		return
	}

	go func() {
		for i := 0; i < 3; i++ {
			var err error
			time.Sleep(250 * time.Millisecond)
			switch i {
			case 0:
				err = kv.Put(key, value0, nil)
			case 1:
				err = kv.Put(key, value1, nil)
			case 2:
				err = kv.Delete(key)
			}
			assert.NoError(t, err)
		}
	}()

	eventCount := 0
	for {
		select {
		case pairs := <-watchCh:
			switch eventCount {
			case 0:
				assert.Nil(t, pairs, "first must be nil, because of we watching a non-exit key")
			case 1:
				if assert.NotNil(t, pairs) {
					assert.Equal(t, value0, pairs[0].Value)
				}
			case 2:
				if assert.NotNil(t, pairs) {
					assert.Equal(t, value1, pairs[0].Value)
				}
			case 3:
				assert.Nil(t, pairs, "last must be nil, because of the key has been deleted")
				close(stopCh)
				return
			}
			eventCount++
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
		}
	}
}
