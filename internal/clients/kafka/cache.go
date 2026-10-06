package kafka

import (
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ClientCache caches a *kadm.Client keyed by a digest of credential bytes.
// If the provided secret changes/rotates, a new client is created.
type ClientCache struct {
	mu           sync.Mutex
	cachedClient *kadm.Client
	credsDigest  [sha256.Size]byte // SHA-256 hash of credentials, avoids storing secret material
}

// GetOrCreate returns the cached client if the credential digest is unchanged,
// otherwise closes the old client and calls newFn to create a new one.
func (c *ClientCache) GetOrCreate(creds []byte, newFn func() (*kadm.Client, error)) (*kadm.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	digest := sha256.Sum256(creds)
	if c.cachedClient != nil && digest == c.credsDigest {
		return c.cachedClient, nil
	}

	svc, err := newFn()
	if err != nil {
		return nil, err
	}

	if svc == nil {
		return nil, errors.New("newFn returned nil client")
	}

	// Only close the old client after successfully creating the new one, ensuring cache
	// consistency even if newFn() fails. Also avoids storing raw secret material.
	if c.cachedClient != nil {
		c.cachedClient.Close()
	}

	c.cachedClient = svc
	c.credsDigest = digest
	return svc, nil
}

// ClientCacheKey identifies the ProviderConfig a cached client belongs to.
// Kind distinguishes ProviderConfig from ClusterProviderConfig, and Namespace
// distinguishes namespaced ProviderConfigs that share the same name.
type ClientCacheKey struct {
	Kind      string
	Namespace string
	Name      string
}

// ClientCaches holds one ClientCache per ProviderConfig identity, so clients for
// different ProviderConfigs (e.g. different Kafka clusters) never evict or close
// each other. Credential rotation is still handled per identity by ClientCache.
type ClientCaches struct {
	mu     sync.Mutex
	caches map[ClientCacheKey]*ClientCache
}

// GetOrCreate returns the cached client for key, creating or rotating it via
// newFn when the credentials for that key have changed. Only the client cached
// for key may be replaced; clients cached for other keys are left untouched.
func (c *ClientCaches) GetOrCreate(key ClientCacheKey, creds []byte, newFn func() (*kadm.Client, error)) (*kadm.Client, error) {
	return c.cacheFor(key).GetOrCreate(creds, newFn)
}

// cacheFor returns the ClientCache for key, creating it if needed. The map lock
// is only held for the lookup so that client creation for one key does not block
// other keys.
func (c *ClientCaches) cacheFor(key ClientCacheKey) *ClientCache {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.caches == nil {
		c.caches = make(map[ClientCacheKey]*ClientCache)
	}
	cc, ok := c.caches[key]
	if !ok {
		cc = &ClientCache{}
		c.caches[key] = cc
	}
	return cc
}
