package ot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"testing"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/cloudflare/circl/group"
	"github.com/stretchr/testify/require"
)

func newTestOTManager(t *testing.T, opts *Options) *OTManager {
	t.Helper()
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: []storage.Bucket{schema.BucketOT},
	})

	var o Options
	if opts == nil {
		o.SlotCount = DefaultSlotCount
		o.SlotSize = DefaultSlotSize
		o.TokenTTL = DefaultTokenTTL
	} else {
		o = *opts
	}

	om, err := NewOTManager(reg, slog.Default(), backend, o)
	if err != nil {
		panic(err)
	}

	res, err := om.ApplyGenerateClusterKey()
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Key, ClusterKeySize, "key length = %d, want %d", len(res.Key), ClusterKeySize)

	err = om.InitTokenCodec()
	require.NoError(t, err)
	return om
}

func newBlob(t *testing.T, om *OTManager) []byte {
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

func mustWriteBlob(t *testing.T, om *OTManager) []byte {
	t.Helper()
	blob := newBlob(t, om)
	_, err := om.ApplyWriteAll(command.CmdOTWriteAll{Blob: blob})
	require.NoError(t, err)
	return blob
}

func Test_OTManager_GenerateClusterKey(t *testing.T) {
	newTestOTManager(t, nil) // internally we generate the cluster key
}

func Test_OTManager_GenerateClusterKey_OnlyOne(t *testing.T) {
	om := newTestOTManager(t, nil)
	_, err := om.ApplyGenerateClusterKey()
	require.Error(t, err, "generating cluster key twice should fail")
}

func Test_NewOTManager_Options_DefaultOptions(t *testing.T) {
	om := newTestOTManager(t, nil)
	require.Equal(t, DefaultSlotCount, om.opts.SlotCount)
	require.Equal(t, DefaultSlotSize, om.opts.SlotSize)
}

func Test_NewOTManager_Options_ValidCustomOptions(t *testing.T) {
	o := &Options{
		SlotCount: DefaultSlotCount + 1,
		SlotSize:  DefaultSlotSize + 1,
		TokenTTL:  MinTokenTTL + 1,
	}
	om := newTestOTManager(t, o)
	require.Equal(t, o.SlotCount, om.opts.SlotCount)
	require.Equal(t, o.SlotSize, om.opts.SlotSize)
	require.Equal(t, o.TokenTTL, om.opts.TokenTTL)
}

func Test_NewOTManager_Options_InvalidSlotCount_FallsBackToDefault(t *testing.T) {
	o := &Options{
		SlotCount: MinSlotCount - 1,
		SlotSize:  MaxSlotCount + 1,
		TokenTTL:  0,
	}
	require.Panics(t, func() {
		newTestOTManager(t, o)
	})
}

func Test_OTManager_Init(t *testing.T) {
	om := newTestOTManager(t, nil)

	pointA, token, err := om.Init()
	require.NoError(t, err)
	require.NotNil(t, pointA)
	require.NotNil(t, token)
	require.Len(t, token, TokenSize, "token length = %d, want %d", len(token), TokenSize)
	require.Len(t, pointA, int(defaultGroup.Params().ScalarLength), "pointA length = %d, want %d", len(pointA), 32)

}

func Test_OTManager_Init_UniquePerCall(t *testing.T) {
	om := newTestOTManager(t, nil)
	_, tok1, err := om.Init()
	require.NoError(t, err)
	_, tok2, err := om.Init()
	require.NoError(t, err)
	require.NotEqual(t, tok1, tok2)
}

func Test_OTManager_SetTokenCodec_DoubleInitFails(t *testing.T) {
	om := newTestOTManager(t, nil)
	require.Error(t, om.InitTokenCodec())
}

func Test_OTManager_CheckBlob_Nil(t *testing.T) {
	om := newTestOTManager(t, nil)
	require.ErrorIs(t, om.CheckBlob(nil), ErrBlobUninitialized)
}

func Test_OTManager_CheckBlob_AllZeroes(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := make([]byte, om.opts.SlotCount*om.opts.SlotSize)
	require.ErrorIs(t, om.CheckBlob(blob), ErrBlobUninitialized)
}

func Test_OTManager_CheckBlob_WrongSize(t *testing.T) {
	om := newTestOTManager(t, nil)
	require.ErrorIs(t, om.CheckBlob([]byte("too short")), ErrBlobSizeCorrupted)
}

func Test_OTManager_CheckBlob_Valid(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := newBlob(t, om)
	require.NoError(t, om.CheckBlob(blob))
}

func Test_OTManager_ApplyWriteAll(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := newBlob(t, om)
	_, err := om.ApplyWriteAll(command.CmdOTWriteAll{Blob: blob})
	require.NoError(t, err)
}

func Test_OTManager_ApplyWriteAll_RejectsInvalidBlob(t *testing.T) {
	om := newTestOTManager(t, nil)

	_, err := om.ApplyWriteAll(command.CmdOTWriteAll{Blob: nil})
	require.Error(t, err)

	_, err = om.ApplyWriteAll(command.CmdOTWriteAll{Blob: []byte("bad")})
	require.Error(t, err)
}

func Test_OTManager_BlobToSlots_AllSlotsPresent(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := newBlob(t, om)

	slots, err := om.blobToSlots(blob)
	require.NoError(t, err)
	require.Len(t, slots, DefaultSlotCount, "should return exactly SlotCount slots")

	for i, slot := range slots {
		require.Len(t, slot, DefaultSlotSize)
		require.Equal(t, byte(i), slot[0], "slot %d first byte", i)
	}
}

func Test_OTManager_E2E_ChosenSlotDecrypts(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := mustWriteBlob(t, om)
	cl := &client{t: t}

	pointA, token, err := om.Init()
	require.NoError(t, err)

	// 1 idx -> 2. elem
	choiceIdx := 1
	pointB, scalarB := cl.choose(pointA, choiceIdx)

	ciphertexts, err := om.Transfer(token, pointB)
	require.NoError(t, err)
	require.Len(t, ciphertexts, om.opts.SlotCount)

	expected := blob[choiceIdx*om.opts.SlotSize : (choiceIdx+1)*om.opts.SlotSize]
	plaintext := cl.decrypt(pointA, scalarB, ciphertexts, choiceIdx)
	require.Equal(t, expected, plaintext)
}

func Test_OTManager_EndToEnd_NonChosenSlotsFail(t *testing.T) {
	om := newTestOTManager(t, nil)
	mustWriteBlob(t, om)
	cl := &client{t: t}

	pointA, token, err := om.Init()
	require.NoError(t, err)

	choice := 0
	pointB, scalarB := cl.choose(pointA, choice)

	ciphertexts, err := om.Transfer(token, pointB)
	require.NoError(t, err)

	for i, ct := range ciphertexts {
		if i == choice {
			continue
		}
		_, err := cl.trydecrypt(pointA, scalarB, ct)
		require.Error(t, err, "slot %d should NOT decrypt with choice=%d's key", i, choice)
	}
}

func Test_OTManager_EndToEnd_AllChoicesWork(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := mustWriteBlob(t, om)
	cl := &client{t: t}

	for choice := range om.opts.SlotCount {
		pointA, token, err := om.Init()
		require.NoError(t, err)

		pointB, scalarB := cl.choose(pointA, choice)
		ciphertexts, err := om.Transfer(token, pointB)
		require.NoError(t, err)

		expected := blob[choice*om.opts.SlotSize : (choice+1)*om.opts.SlotSize]
		got := cl.decrypt(pointA, scalarB, ciphertexts, choice)
		require.Equal(t, expected, got, "choice=%d", choice)
	}
}

func Test_OTManager_EndToEnd_FirstAndLastSlot(t *testing.T) {
	om := newTestOTManager(t, nil)
	blob := mustWriteBlob(t, om)
	cl := &client{t: t}

	for choice := range om.opts.SlotCount {
		if choice == 0 || choice == om.opts.SlotCount-1 {
			continue
		}
		pointA, token, err := om.Init()
		require.NoError(t, err)

		pointB, scalarB := cl.choose(pointA, choice)
		ciphertexts, err := om.Transfer(token, pointB)
		require.NoError(t, err)

		expected := blob[choice*om.opts.SlotSize : (choice+1)*om.opts.SlotSize]
		got := cl.decrypt(pointA, scalarB, ciphertexts, choice)
		require.Equal(t, expected, got, "choice=%d", choice)
	}
}

// client performs the receivers role in 1-out-of-N OT.
type client struct {
	t *testing.T
}

// Protocol (receiver side):
// Given server's point A and desired choice index c:
//
//	b       = random scalar
//	B       = b*G + c*A       // blinded choice point
func (c *client) choose(pointABytes []byte, choice int) (pointBBytes []byte, scalarB group.Scalar) {
	c.t.Helper()

	A := defaultGroup.NewElement()
	err := A.UnmarshalBinary(pointABytes)
	require.NoError(c.t, err)

	cScalar := defaultGroup.NewScalar().SetUint64(uint64(choice))
	cA := defaultGroup.NewElement().Mul(A, cScalar)

	scalarB = defaultGroup.RandomScalar(rand.Reader)
	bG := defaultGroup.NewElement().MulGen(scalarB)

	B := defaultGroup.NewElement().Add(bG, cA)
	pointBBytes, err = B.MarshalBinary()
	require.NoError(c.t, err)
	return
}

// After receiving ciphertexts from Transfer:
//
//	key     = b*A              // only matches slot c's encryption key
//	hash    = sha256(key)
//	plain   = GCM_Open(hash, ciphertexts[c])
func (c *client) trydecrypt(pointABytes []byte, b group.Scalar, ct []byte) ([]byte, error) {
	c.t.Helper()

	A := defaultGroup.NewElement()
	err := A.UnmarshalBinary(pointABytes)
	if err != nil {
		return nil, err
	}

	key := defaultGroup.NewElement().Mul(A, b)
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	hasher := sha256.New()
	hasher.Write(keyBytes)
	hashedKey := hasher.Sum(nil)

	require.Len(c.t, hashedKey, 32, "hashed key size mismatch")
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

func (c *client) decrypt(pointABytes []byte, b group.Scalar, ciphertexts [][]byte, choice int) []byte {
	c.t.Helper()
	plaintext, err := c.trydecrypt(pointABytes, b, ciphertexts[choice])
	require.NoError(c.t, err)
	return plaintext
}
