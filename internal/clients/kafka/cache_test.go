package kafka

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestGetOrCreateCacheHit verifies that cached clients are reused with same credentials.
func TestGetOrCreateCacheHit(t *testing.T) {
	cache := &ClientCache{}
	creds := []byte("secret123")
	var callCount int32

	newFn := func() (*kadm.Client, error) {
		atomic.AddInt32(&callCount, 1)
		return &kadm.Client{}, nil
	}

	client1, err := cache.GetOrCreate(creds, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))

	client2, err := cache.GetOrCreate(creds, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount)) // Should not call newFn again
	assert.Same(t, client1, client2)
}

// TestGetOrCreateErrorHandling verifies that errors from newFn are propagated.
func TestGetOrCreateErrorHandling(t *testing.T) {
	cache := &ClientCache{}
	creds := []byte("secret")
	testErr := errors.New("creation failed")

	newFn := func() (*kadm.Client, error) {
		return nil, testErr
	}

	_, err := cache.GetOrCreate(creds, newFn)
	require.Error(t, err)
	assert.Equal(t, testErr, err)

	// Cache should be empty after error
	assert.Nil(t, cache.cachedClient)
}

// TestGetOrCreateConcurrentAccess verifies thread-safety with concurrent calls.
func TestGetOrCreateConcurrentAccess(t *testing.T) {
	cache := &ClientCache{}
	creds := []byte("secret")
	var creationCount int32

	newFn := func() (*kadm.Client, error) {
		atomic.AddInt32(&creationCount, 1)
		return &kadm.Client{}, nil
	}

	// Launch multiple goroutines requesting the same credentials
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := cache.GetOrCreate(creds, newFn)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	// With the lock held during newFn(), only 1 client should be created
	assert.Equal(t, int32(1), atomic.LoadInt32(&creationCount))
	assert.NotNil(t, cache.cachedClient)
}

// TestGetOrCreateEmptyCredentials verifies behavior with empty credential bytes.
func TestGetOrCreateEmptyCredentials(t *testing.T) {
	cache := &ClientCache{}
	emptyCreds := []byte{}
	var callCount int32

	newFn := func() (*kadm.Client, error) {
		atomic.AddInt32(&callCount, 1)
		return &kadm.Client{}, nil
	}

	_, err := cache.GetOrCreate(emptyCreds, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))

	_, err = cache.GetOrCreate(emptyCreds, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount)) // Should reuse cached client
}

// TestGetOrCreateCredentialComparison verifies that credential comparison is byte-exact.
func TestGetOrCreateCredentialComparison(t *testing.T) {
	cache := &ClientCache{}
	creds1 := []byte("secret")
	creds2 := []byte("secret")
	var callCount int32

	newFn := func() (*kadm.Client, error) {
		atomic.AddInt32(&callCount, 1)
		return &kadm.Client{}, nil
	}

	// Same credentials (different objects, same content)
	_, err := cache.GetOrCreate(creds1, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))

	// Should reuse client even though it's a different object
	_, err = cache.GetOrCreate(creds2, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))
}

// TestGetOrCreateErrorPreservesCacheState verifies that cache remains unchanged if newFn fails.
func TestGetOrCreateErrorPreservesCacheState(t *testing.T) {
	cache := &ClientCache{}
	creds1 := []byte("secret1")
	creds2 := []byte("secret2")
	var callCount int32

	newFn := func() (*kadm.Client, error) {
		atomic.AddInt32(&callCount, 1)
		return &kadm.Client{}, nil
	}

	// Create initial client with creds1
	client1, err := cache.GetOrCreate(creds1, newFn)
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))
	originalDigest := cache.credsDigest

	// Try to rotate to creds2, but newFn fails
	failingFn := func() (*kadm.Client, error) {
		return nil, errors.New("connection failed")
	}

	_, err = cache.GetOrCreate(creds2, failingFn)
	require.Error(t, err)

	// Verify cache is unchanged - still has original client and digest
	assert.Same(t, client1, cache.cachedClient)
	assert.Equal(t, originalDigest, cache.credsDigest)
}

// TestGetOrCreateNilClientRejected verifies that a nil client is treated as an error.
func TestGetOrCreateNilClientRejected(t *testing.T) {
	cache := &ClientCache{}
	creds := []byte("secret")

	nilClientFn := func() (*kadm.Client, error) {
		return nil, nil
	}

	_, err := cache.GetOrCreate(creds, nilClientFn)
	require.Error(t, err)
	assert.Nil(t, cache.cachedClient)
}

const (
	kindProviderConfig        = "ProviderConfig"
	kindClusterProviderConfig = "ClusterProviderConfig"
	testPCName                = "kafka"
)

// newClosableTestClient returns a real (never connected) kadm client whose
// underlying kgo context is canceled when the client is closed.
func newClosableTestClient(t *testing.T) (*kadm.Client, *kgo.Client) {
	t.Helper()
	kcl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(kcl.Close)
	return kadm.NewClient(kcl), kcl
}

func isClosed(kcl *kgo.Client) bool {
	return kcl.Context().Err() != nil
}

// TestClientCachesIsolatesKeys verifies that connecting with a different
// ProviderConfig identity neither replaces nor closes another identity's client.
func TestClientCachesIsolatesKeys(t *testing.T) {
	t.Parallel()

	caches := &ClientCaches{}
	keyA := ClientCacheKey{Kind: kindProviderConfig, Namespace: "team-a", Name: testPCName}
	keyB := ClientCacheKey{Kind: kindProviderConfig, Namespace: "team-b", Name: testPCName}

	admA, kclA := newClosableTestClient(t)
	admB, kclB := newClosableTestClient(t)

	gotA, err := caches.GetOrCreate(keyA, []byte(`{"brokers":["a:9092"]}`), func() (*kadm.Client, error) { return admA, nil })
	require.NoError(t, err)
	gotB, err := caches.GetOrCreate(keyB, []byte(`{"brokers":["b:9092"]}`), func() (*kadm.Client, error) { return admB, nil })
	require.NoError(t, err)

	assert.Same(t, admA, gotA)
	assert.Same(t, admB, gotB)
	assert.False(t, isClosed(kclA), "client for keyA must not be closed when keyB connects")
	assert.False(t, isClosed(kclB))

	// Interleaved reconciles keep reusing each identity's client.
	for range 3 {
		got, err := caches.GetOrCreate(keyA, []byte(`{"brokers":["a:9092"]}`), func() (*kadm.Client, error) {
			t.Fatal("newFn must not be called for cached keyA")
			return nil, nil
		})
		require.NoError(t, err)
		assert.Same(t, admA, got)

		got, err = caches.GetOrCreate(keyB, []byte(`{"brokers":["b:9092"]}`), func() (*kadm.Client, error) {
			t.Fatal("newFn must not be called for cached keyB")
			return nil, nil
		})
		require.NoError(t, err)
		assert.Same(t, admB, got)
	}
	assert.False(t, isClosed(kclA))
	assert.False(t, isClosed(kclB))
}

// TestClientCachesKeyIdentity verifies that every component of the key is
// significant, so equally named configs of different kinds or namespaces never
// share a client.
func TestClientCachesKeyIdentity(t *testing.T) {
	t.Parallel()

	caches := &ClientCaches{}
	keys := []ClientCacheKey{
		{Kind: kindProviderConfig, Namespace: "team-a", Name: testPCName},
		{Kind: kindProviderConfig, Namespace: "team-b", Name: testPCName},
		{Kind: kindClusterProviderConfig, Name: testPCName},
		{Kind: kindProviderConfig, Name: testPCName},
	}
	creds := []byte("same-creds")
	var callCount int32

	clients := make(map[*kadm.Client]struct{})
	for _, k := range keys {
		got, err := caches.GetOrCreate(k, creds, func() (*kadm.Client, error) {
			atomic.AddInt32(&callCount, 1)
			return &kadm.Client{}, nil
		})
		require.NoError(t, err)
		clients[got] = struct{}{}
	}

	assert.Equal(t, int32(len(keys)), atomic.LoadInt32(&callCount))
	assert.Len(t, clients, len(keys))
}

// TestClientCachesRotationScopedToKey verifies that a credential rotation for
// one identity replaces and closes only that identity's client.
func TestClientCachesRotationScopedToKey(t *testing.T) {
	t.Parallel()

	caches := &ClientCaches{}
	keyA := ClientCacheKey{Kind: kindClusterProviderConfig, Name: "cluster-a"}
	keyB := ClientCacheKey{Kind: kindClusterProviderConfig, Name: "cluster-b"}

	admA1, kclA1 := newClosableTestClient(t)
	admA2, kclA2 := newClosableTestClient(t)
	admB, kclB := newClosableTestClient(t)

	_, err := caches.GetOrCreate(keyA, []byte("a-v1"), func() (*kadm.Client, error) { return admA1, nil })
	require.NoError(t, err)
	_, err = caches.GetOrCreate(keyB, []byte("b-v1"), func() (*kadm.Client, error) { return admB, nil })
	require.NoError(t, err)

	got, err := caches.GetOrCreate(keyA, []byte("a-v2"), func() (*kadm.Client, error) { return admA2, nil })
	require.NoError(t, err)
	assert.Same(t, admA2, got)
	assert.True(t, isClosed(kclA1), "rotated client for keyA must be closed")
	assert.False(t, isClosed(kclA2))
	assert.False(t, isClosed(kclB), "client for keyB must not be affected by keyA rotation")

	got, err = caches.GetOrCreate(keyB, []byte("b-v1"), func() (*kadm.Client, error) {
		t.Fatal("newFn must not be called for unchanged keyB")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Same(t, admB, got)
}

// TestClientCachesConcurrentKeys verifies thread-safety across many keys: each
// identity gets exactly one client regardless of concurrent access.
func TestClientCachesConcurrentKeys(t *testing.T) {
	t.Parallel()

	caches := &ClientCaches{}
	keys := []ClientCacheKey{
		{Kind: kindProviderConfig, Namespace: "a", Name: "pc"},
		{Kind: kindProviderConfig, Namespace: "b", Name: "pc"},
		{Kind: kindClusterProviderConfig, Name: "pc"},
	}
	var creationCount int32

	const perKey = 10
	var wg sync.WaitGroup
	results := make([][]*kadm.Client, len(keys))
	for i := range keys {
		results[i] = make([]*kadm.Client, perKey)
	}
	for i, k := range keys {
		for j := range perKey {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := caches.GetOrCreate(k, []byte("creds"), func() (*kadm.Client, error) {
					atomic.AddInt32(&creationCount, 1)
					return &kadm.Client{}, nil
				})
				assert.NoError(t, err)
				results[i][j] = got
			}()
		}
	}
	wg.Wait()

	assert.Equal(t, int32(len(keys)), atomic.LoadInt32(&creationCount))
	for i := range keys {
		for j := range perKey {
			assert.Same(t, results[i][0], results[i][j])
		}
	}
	assert.NotSame(t, results[0][0], results[1][0])
	assert.NotSame(t, results[1][0], results[2][0])
}
