package dynamoDB

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tskinn/libkv"
	"github.com/tskinn/libkv/store"
	"strings"
	"time"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Dynamodb
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb does not support multiple endpoints")
)

type DynamoDB struct {
	tableName string
	db        *dynamodb.DynamoDB
}

//
func Register() {
	libkv.AddStore(store.DYNAMODB, New)
}

// create a new connection to dynamodb store
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	dynamo := &DynamoDB
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	dynamo.db = dynamodb.New(sess)
	return dynamo, nil
}

//
func (d *DynamoDB) Get(key string) (*store.KVPair, error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			S: aws.String(key),
		},
		TableName: aws.String(db.tableName),
	}

	resp, err := d.db.GetItem(input * dynamodb.GetItemInput)
	if err != nil {
		return nil, err
	}
	return &store.KVPair{
		Value: resp.String(),
		Key:   key,
	}, nil
}

//
func (d *DynamoDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	// TODO take into account the WriteOptions
	params := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			key: {
				S: aws.String(string(value[:])),
			},
		},
		TableName: aws.String(d.tableName),
	}

	resp, err := d.db.PutItem(params)
	if err != nil {
		return err
	}

	return nil
}

//
func (d *DynamoDB) Delete(key string) error {
	// TODO
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			S: aws.String(key),
		},
		TableName: aws.String(d.tableName),
	}

	resp, err := d.db.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

//
func (d *DynamoDB) Exists(key string) (bool, error) {
	_, err := d.Get(key)
	if err == nil {
		return true
	}
	if strings.Contains(err.Error(), "ResourceNotFound") {
		return false, nil
	}
	return false, err
}

//
func (d *DynamoDB) List(directory string) ([]*store.KVPair, error) {
	pairs := make([]*store.KVPair, 0)
	params := &dynamodb.ScanInput{
		FilterExpression: aws.String("begins_with(key, " + directory + ")"),
		TableName:        aws.String(d.tableName),
	}

	resp, err := d.db.Scan(params)
	if err != nil {
		return pairs, err
	}

	for _, item := range resp.Items {
		for key, value := range item {
			tPair := &store.KVPair{
				Key:   key,
				Value: *value.S,
			}
			pairs = append(pairs, tPair)
		}
	}

	return pairs, nil
}

//
func (d *DynamoDB) DeleteTree(directory string) error {
	retryList := make([]*store.KVPair, 0)
	pairs, err := d.List(directory)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		err = d.Delete(pair.Key)
		if err != nil {
			retryList = append(retryList, pair)
		}
	}
	// TODO maybe retry deletes
	if len(retryList) > 0 {
		return fmt.Errorf("Unable to delete all of the tree: %v", retryList)
	}
	return nil
}

//
// TODO investigate using dynamodb streams. Would require more outside coordination
func (d *DynamoDB) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {

}

//
// TODO investigate using dynamodb streams. Would require more outside coordination
func (d *DynamoDB) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {

}

// Not supported
func (d *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("NewLock not supported")
}

// Not supported
func (d *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, errors.New("AtomicPut not supported")
}

// Not supported
func (d *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return false, errors.New("AtomicDelete not supported")
}

func (d *DynamoDB) Close() {
	return
}
