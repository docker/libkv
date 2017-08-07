package memo

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/testutils"
	"testing"
)

func TestMemoStore(t *testing.T) {
	Register()
	kv, err := libkv.NewStore("memo", []string{"localhost:9000"}, nil)
	if err != nil {
		t.Fatal("error instantiating memo")
	}
	testutils.RunTestCommon(t, kv)
	testutils.RunCleanup(t, kv)
}
