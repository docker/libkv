package etcd

import (
	"testing"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
	"crypto/tls"
	"crypto/x509"
)

var (
	client = "localhost:4001"
)

func makeEtcdClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "test",
			Password:          "very-secure",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := libkv.NewStore(store.ETCD, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Etcd); !ok {
		t.Fatal("Error registering and initializing etcd")
	}
}

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}

func TestNew(t *testing.T){
	caCertRoot := `-----BEGIN CERTIFICATE-----
MIID3TCCAsWgAwIBAgIJALf1/TqTDCA8MA0GCSqGSIb3DQEBCwUAMIGEMQswCQYD
VQQGEwJwazEOMAwGA1UECAwFc2luZGgxEDAOBgNVBAcMB2thcmFjaGkxETAPBgNV
BAoMCDEwcGVhcmxzMQwwCgYDVQQLDAMxMHAxDjAMBgNVBAMMBXVtYWlyMSIwIAYJ
KoZIhvcNAQkBFhN1bWFpckB0ZW5wZWFybHMuY29tMB4XDTE3MDQxMjEwNDgxMloX
DTI3MDQxMDEwNDgxMlowgYQxCzAJBgNVBAYTAnBrMQ4wDAYDVQQIDAVzaW5kaDEQ
MA4GA1UEBwwHa2FyYWNoaTERMA8GA1UECgwIMTBwZWFybHMxDDAKBgNVBAsMAzEw
cDEOMAwGA1UEAwwFdW1haXIxIjAgBgkqhkiG9w0BCQEWE3VtYWlyQHRlbnBlYXJs
cy5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDGxsXnsb3duKEn
pZKz2QDeuf7FrHyY1gKOOWJpUDr6ldTBOHxYlKWIeWu4Df17Zc6kGOUIPIM/g6j3
LKcvQcGetZHc6Ha7Zbxufzn4/Ye2wXumbrj54R+d6Tc1sf7eylbcZZt8GsOMoRGL
LIBKD3uEG9b9XW6wOkz2eYQmpKHfPc3Z20DPkwmPrFY4p9MnVbrLucmMA3R5Iqyo
wTwCfdMgE6NzGXlTUbBYoa30K8y5DCWjsBDLRw/j6dGGxG0eRB5BnDOaWQKGCAEQ
aeYSKOVhmtVQxrj9HpA+g9NmcqFCnP7MTp8ldcZIMlnB8KcTbNPt9EROa22g5BLb
la4nWJ3RAgMBAAGjUDBOMB0GA1UdDgQWBBS54WSjCLenmRAgfXLewVSL6RDkpzAf
BgNVHSMEGDAWgBS54WSjCLenmRAgfXLewVSL6RDkpzAMBgNVHRMEBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4IBAQAmtD8Eu67UMejoomnBHK0wNt3/e48NZDtyNNqsyZjU
wPMXMXcZTTzWOEQz956ct6DTzPS2X+pd+QdzGG8Be9WJYIEFHad/AgnEx2/C8CaY
Bg+FkGulnaS3aheKpCzL7T5bA0j50tr3QJVJbLzrf+zZU1ySdufttuCwstgVKlnx
wUSHPGdqh/LbpQMOFiWMG8xF1YMSY2cAzlSnXdOiYbnYQqIR/Up33G/abJPNstu4
77kNW8lSa6J5nIuwnFWGyCyXc6ZAjpQOIQsSUHytvSxxWZIpe60yEbC40Vkal1nv
UCsXdmBhlyjVRLF3Qr6KT4p7zmtFNVL3f/sDykvUKaSG
-----END CERTIFICATE-----`

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCertRoot))

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		ClientCAs: caCertPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			TLS: tlsConfig,
			Username: "SomeRandomString",
			Bucket: "somebucket",
			PersistConnection: true,
			Password: "randomPassword",
		},
	)

	assert.Nil(t, err);
	assert.NotEmpty(t, kv);
}