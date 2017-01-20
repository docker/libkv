package dynamo

import (
	"github.com/stretchr/testify/assert"
	"github.com/tskinn/libkv"
	"github.com/tskinn/libkv/store"
	"github.com/tskinn/libkv/testutils"
	"testing"
)

var client = "traefik"

func makeDynamoClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			Bucket: "us-east-1",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()
	kv, err := libkv.NewStore(store.DYNAMODB, []string{client}, &store.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}
}

func TestDynamoDBStore(t *testing.T) {
	kv := makeDynamoClient(t)
	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunCleanup(t, kv)
}
