package dynamo

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tskinn/libkv"
	"github.com/tskinn/libkv/store"
	"strconv"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Dynamodb
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb does not support multiple endpoints")
)

type DynamoDB struct {
	tableName string
	client    *dynamodb.DynamoDB
}

// Register registers dynamodb to libkv
func Register() {
	libkv.AddStore(store.DYNAMODB, New)
}

// New create a new connection to dynamodb then table named endpoint
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	// treate the bucket as the AWS region
	// default to us-east-1
	region := "us-east-1"
	if options.Bucket != "" {
		region = options.Bucket
	}

	var sess *session.Session
	var creds *credentials.Credentials

	// If creds are provided use those
	// Treate Username as AWS_ACCESS_KEY_ID and Password as AWS_SECRET_ACCESSK_EY
	if options.Username != "" && options.Password != "" {
		creds = credentials.NewStaticCredentials(options.Username, options.Password, "")
		sess, _ = session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: creds,
		})
	} else {
		sess, _ = session.NewSession(&aws.Config{Region: aws.String(region)})
	}

	dyna := &DynamoDB{
		tableName: endpoints[0],
		client:    dynamodb.New(sess),
	}
	return dyna, nil
}

//
func (d *DynamoDB) Get(key string) (*store.KVPair, error) {
	pair := &store.KVPair{}
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	resp, err := d.client.GetItem(params)
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, store.ErrKeyNotFound
	}

	pair.Key = key
	pair.Value = []byte(*resp.Item["Value"].S)
	pair.LastIndex, _ = strconv.ParseUint(*resp.Item["Index"].N, 10, 64)
	return pair, nil
}

//
func (d *DynamoDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	params := &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": &dynamodb.AttributeValue{
				S: aws.String(key),
			},
		},
		UpdateExpression: aws.String("set #v = :v add #i :i"),
		ExpressionAttributeNames: map[string]*string{
			"#v": aws.String("Value"),
			"#i": aws.String("Index"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": &dynamodb.AttributeValue{
				S: aws.String(string(value[:])),
			},
			":i": &dynamodb.AttributeValue{
				N: aws.String("1"),
			},
		},
	}

	_, err := d.client.UpdateItem(params)
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
			"Key": &dynamodb.AttributeValue{
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	_, err := d.client.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

//
func (d *DynamoDB) Exists(key string) (bool, error) {
	pair, err := d.Get(key)
	if pair != nil && pair.Key == "" || err == store.ErrKeyNotFound {
		return false, nil
	} else if err == nil {
		return true, nil
	}
	return false, err
}

//
func (d *DynamoDB) List(directory string) ([]*store.KVPair, error) {
	pairs := make([]*store.KVPair, 0)
	params := &dynamodb.ScanInput{
		FilterExpression: aws.String("begins_with( #k, :v)"),
		TableName:        aws.String(d.tableName),
		ExpressionAttributeNames: map[string]*string{
			"#k": aws.String("Key"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": &dynamodb.AttributeValue{
				S: aws.String(directory),
			},
		},
	}
	// TODO is scan the best way to do this?
	// Maybe a refactor of the key value format will allow
	// a more efficient queuery to be used or something?
	resp, err := d.client.Scan(params)
	if err != nil {
		return nil, err
	}
	if len(resp.Items) == 0 {
		return nil, store.ErrKeyNotFound
	}
	for _, item := range resp.Items {
		tPair := &store.KVPair{
			Key:   *item["Key"].S,
			Value: []byte(*item["Value"].S),
		}
		pairs = append(pairs, tPair)
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

// Watch has to be implemented at the library level or be hooked up to a dynamodb stream
//   which might not be likely since AWS only suggests at most two processes reading
//   from a dynamodb stream
func (d *DynamoDB) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, errors.New("Watch not supported")
}

// WatchTree has to be implemented at the library since it is not natively supportedby dynamoDB
func (d *DynamoDB) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, errors.New("WatchTree not supported")
}

// Not supported
func (d *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("NewLock not supported")
}

// Not supported
func (d *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	// TODO use a conditional update and check if values are are same and put
	return false, nil, errors.New("AtomicPut not supported")
}

// Not supported
func (d *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	// TODO use a conditional update and check if values are are same and delete
	return false, errors.New("AtomicDelete not supported")
}

func (d *DynamoDB) Close() {
	return
}
