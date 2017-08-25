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
}
