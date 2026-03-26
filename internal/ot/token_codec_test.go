package ot

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestCodec(t *testing.T, ttl int64) *tokenCodec {
	t.Helper()
	clusterKey := make([]byte, ClusterKeySize)
	_, err := rand.Read(clusterKey)
	require.NoError(t, err)
	tc, err := newTokenCodec(clusterKey, ttl)
	require.NoError(t, err)
	return tc
}

func Test_NewTokenCodec_InvalidKeySize(t *testing.T) {
	for _, size := range []int{0, 15, 16, 24, 31, 33, 64} {
		key := make([]byte, size)
		_, err := newTokenCodec(key, DefaultTokenTTL)
		if err != ErrInvalidClusterKeySize {
			t.Errorf("key size %d: expected ErrInvalidClusterKeySize, got: %v", size, err)
		}
	}
}

func Test_SealOpen_RoundTrip(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	_, err := rand.Read(scalar)
	require.NoError(t, err, "crypto/rand")

	token, err := tc.seal(scalar)
	require.NoError(t, err, "seal")

	got, err := tc.open(token)
	require.NoError(t, err, "open")
	require.Equal(t, len(scalar), len(got), "expected scalar length %d, got %d", len(scalar), len(got))

	for i := range got {
		require.Equal(t, scalar[i], got[i], "scalar[%d] = %d, want %d", i, got[i], scalar[i])
	}
}

func Test_SealOpen_DifferentScalarSizes(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	// The codec only works with scalars of size 32
	// This has to be refactors if we ever switch curves or add extra fields.
	for _, size := range []int{8, 16, 48, 64, 128, 256} {
		scalar := make([]byte, size)
		rand.Read(scalar)

		_, err := tc.seal(scalar)
		require.Error(t, err, "expected seal(size=%d) to fail", size)
	}
}

func Test_Open_ExpiredToken(t *testing.T) {
	ttl := int64(1)
	tc := newTestCodec(t, ttl)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	token, err := tc.seal(scalar)
	require.NoError(t, err, "seal")

	time.Sleep(time.Duration(2*ttl) * time.Second)

	_, err = tc.open(token)
	require.ErrorIs(t, err, ErrTokenExpired, "expected ErrTokenExpired, got: %v", err)
}

func Test_Open_TamperedToken(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	token, err := tc.seal(scalar)
	require.NoError(t, err, "seal")

	// little bit of tampering (after nonce only)
	token[nonceSize+2] ^= 0xFF

	_, err = tc.open(token)
	require.ErrorIs(t, err, ErrTokenTampered, "expected ErrTokenTampered, got: %v", err)
}

func Test_Open_WrongKey(t *testing.T) {
	// Create two codecs with different cluster keys.
	// A token sealed by one should not open with the other.
	tc1 := newTestCodec(t, DefaultTokenTTL)
	tc2 := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	token, err := tc1.seal(scalar)
	require.NoError(t, err, "seal")

	_, err = tc2.open(token)
	require.ErrorIs(t, err, ErrTokenTampered, "expected ErrTokenTampered when using wrong key, got: %v", err)
}

func Test_Open_TruncatedToken(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	token, err := tc.seal(scalar)
	require.NoError(t, err, "seal")

	_, err = tc.open(nil)
	require.ErrorIs(t, err, ErrInvalidTokenSize, "expected ErrInvalidTokenSize for empty input, got: %v", err)

	_, err = tc.open(token[:nonceSize])
	require.ErrorIs(t, err, ErrInvalidTokenSize, "expected ErrInvalidTokenSize for smaller input, got: %v", err)
}

func Test_Open_TamperedNonce(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	token, err := tc.seal(scalar)
	require.NoError(t, err, "seal")

	// little tamper in the nonce: GCM authentication tag wont match
	token[0] ^= 0x01

	_, err = tc.open(token)
	require.ErrorIs(t, err, ErrTokenTampered, "expected ErrTokenTampered when nonce is modified, got: %v", err)
}

func Test_Seal_UniqueTokensEachCall(t *testing.T) {
	tc := newTestCodec(t, DefaultTokenTTL)

	scalar := make([]byte, scalarSize)
	rand.Read(scalar)

	// sealing the same scalar will produce different tokens
	// since each seal creates a random nonce
	token1, err := tc.seal(scalar)
	require.NoError(t, err, "seal1")
	token2, err := tc.seal(scalar)
	require.NoError(t, err, "seal2")

	require.Equal(t, len(token1), len(token2), "tokens should be the same length")
	require.NotEqual(t, token1, token2, "two seals of the same scalar produced identical tokens")
}
