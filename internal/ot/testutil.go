package ot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/cloudflare/circl/group"
	"github.com/stretchr/testify/require"
)

// MockOTClient performs the receivers role in 1-out-of-N OT.
type MockOTClient struct {
	T *testing.T
}

func FakeBlob(t *testing.T, om *OTManager) []byte {
	t.Helper()
	blob := make([]byte, om.opts.SlotCount*om.opts.SlotSize)
	for i := range om.opts.SlotCount {
		offset := i * om.opts.SlotSize

		blob[offset] = byte(i)
		for j := range om.opts.SlotSize {
			blob[offset+j] = byte(i + j)
		}
	}
	return blob
}

// Protocol (receiver side):
// Given server's point A and desired choice index c:
//
//	b       = random scalar
//	B       = b*G + c*A       // blinded choice point
func (c *MockOTClient) Choose(pointABytes []byte, choice int) (pointBBytes []byte, scalarB group.Scalar) {
	c.T.Helper()

	A := Group.NewElement()
	err := A.UnmarshalBinary(pointABytes)
	require.NoError(c.T, err)

	cScalar := Group.NewScalar().SetUint64(uint64(choice))
	cA := Group.NewElement().Mul(A, cScalar)

	scalarB = Group.RandomScalar(rand.Reader)
	bG := Group.NewElement().MulGen(scalarB)

	B := Group.NewElement().Add(bG, cA)
	pointBBytes, err = B.MarshalBinary()
	require.NoError(c.T, err)
	return
}

// After receiving ciphertexts from Transfer:
//
//	key     = b*A              // only matches slot c's encryption key
//	hash    = sha256(key)
//	plain   = GCM_Open(hash, ciphertexts[c])
func (c *MockOTClient) TryDecrypt(pointABytes []byte, b group.Scalar, ct []byte) ([]byte, error) {
	c.T.Helper()

	A := Group.NewElement()
	err := A.UnmarshalBinary(pointABytes)
	if err != nil {
		return nil, err
	}

	key := Group.NewElement().Mul(A, b)
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	hasher := sha256.New()
	hasher.Write(keyBytes)
	hashedKey := hasher.Sum(nil)

	require.Len(c.T, hashedKey, 32, "hashed key size mismatch")
	if len(hashedKey) != 32 {
		return nil, err
	}

	block, err := aes.NewCipher(hashedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher blocka: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := ct[:aead.NonceSize()]
	ciphertext := ct[aead.NonceSize():]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed: %v", err)
	}

	return plaintext, nil
}

func (c *MockOTClient) Decrypt(pointABytes []byte, b group.Scalar, ciphertexts [][]byte, choice int) []byte {
	c.T.Helper()
	plaintext, err := c.TryDecrypt(pointABytes, b, ciphertexts[choice])
	require.NoError(c.T, err)
	return plaintext
}
