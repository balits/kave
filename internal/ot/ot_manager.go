package ot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/cloudflare/circl/group"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	MinSlotCount     int = 16
	DefaultSlotCount int = 64
	MaxSlotCount     int = 512

	MinSlotSize     int = 32
	DefaultSlotSize int = 256
	MaxSlotSize     int = 4096

	MinTokenTTL int64 = 15 // Min time-to-live for any token in secons
	MaxTokenTTL int64 = 60 // Max time-to-live for any token in secons
)

var (
	ErrClusterKeyGen     = errors.New("failed to generate cluter key")
	ErrBlobUninitialized = errors.New("blob is uninitialized (nil or all zeroes)")
	ErrBlobSizeCorrupted = errors.New("blob size is malformed, expected SlotCount * SlotSize")

	// Default package wide group used for eliptic curve math
	Group = group.Ristretto255
)

type Options struct {
	SlotCount int   `json:"slot_count"`
	SlotSize  int   `json:"slot_size"`
	TokenTTL  int64 `json:"token_ttl_sec"`
}

var DefaultOptions = Options{
	SlotCount: DefaultSlotCount,
	SlotSize:  DefaultSlotSize,
	TokenTTL:  DefaultTokenTTL,
}

func (o *Options) Check() error {
	if o.SlotCount < MinSlotCount || o.SlotCount > MaxSlotCount {
		return fmt.Errorf("invalid slot count (min=%d, max=%d)", MinSlotCount, MaxSlotCount)
	}
	if o.SlotSize < MinSlotSize || o.SlotSize > MaxSlotSize {
		return fmt.Errorf("invalid slot size (min=%d, max=%d)", MinSlotSize, MaxSlotSize)
	}
	if o.TokenTTL < MinTokenTTL || o.TokenTTL > MaxTokenTTL {
		return fmt.Errorf("invalid token ttl (min=%d, max=%d)", MinTokenTTL, MaxTokenTTL)
	}
	return nil
}

type ReadOnlyOT interface {
	Init() (pointA, token []byte, err error)
	Transfer(token, pointB []byte) (ciphertexts [][]byte, err error)
	CheckBlob(blob []byte) error
}

type OTManager struct {
	opts    Options
	codec   *tokenCodec
	backend backend.Backend
	metrics *metrics.OTMetrics
	logger  *slog.Logger
}

func NewOTManager(reg prometheus.Registerer, logger *slog.Logger, backend backend.Backend, opts Options) (*OTManager, error) {
	if err := opts.Check(); err != nil {
		return nil, err
	}

	return &OTManager{
		opts:    opts,
		codec:   nil, // codec needs a cluster key -> two phase init
		backend: backend,
		metrics: metrics.NewOTMetrics(reg),
		logger:  logger.With("component", "ot_manager"),
	}, nil
}

// TODO: figure out the bootstrap/join sequence
//
// InitTokenCodec initializes the token codec, setting its key to the cluster key stored in the backend
// and its maxTTL to the provided ttl (in seconds).
//
// If the key is not found or invalid, an error is returned and the codec remains uninitialized.
// If the codec is already initialized, an error is returned to prevent overwriting it.
// This method should be called after the OTManager is created and before any OT operations are performed.
//
// NOTE: this method is the mandatory part of the two phase initialization of the OTManager
// and is separated from the constructor because the cluster key needs to be generated and stored
// in a separate step (OTManager.ApplyGenerateClusterKey through the FSM so its replicated across all nodes)
// before it can be used to initialize the codec.
func (om *OTManager) InitTokenCodec() error {
	rtx := om.backend.ReadTx()
	rtx.RLock()
	defer rtx.RUnlock()
	clusterKey, err := rtx.UnsafeGet(schema.BucketOT, schema.KeyOTClusterKey)
	if err != nil {
		return fmt.Errorf("init token codec error: failed to retrieve cluster key: %w", err)
	} else if clusterKey == nil {
		return errors.New("init token codec error: cluster key is not set")
	}

	tc, err := newTokenCodec(clusterKey, om.opts.TokenTTL)
	if err != nil {
		return fmt.Errorf("init token codec error: %w", err)
	}
	om.codec = tc
	return nil
}

// Init generates a new token and returns it and the servers public point A.
// The token contains the servers scalar a, encrypted with the cluster key,
// and the timestamp of the generation, which validates this token for use only for the next OTManager.TokenCodec.MaxTTL seconds.
// The the point A is computed as a*G where G is the group generator.
func (om *OTManager) Init() (pointA, token []byte, err error) {
	start := time.Now()
	om.metrics.InitCount.Inc()
	defer func() { om.metrics.InitDurationSec.Observe(time.Since(start).Seconds()) }()

	a := Group.RandomScalar(rand.Reader)
	A := Group.NewElement().MulGen(a)

	aBytes, err := a.MarshalBinary()
	if err != nil {
		om.metrics.InitErrorsTotal.Inc()
		return nil, nil, fmt.Errorf("ot error: %w", err)
	}

	token, err = om.codec.seal(aBytes)
	if err != nil {
		om.metrics.InitErrorsTotal.Inc()
		return nil, nil, fmt.Errorf("ot error: %w", err)
	}

	pointA, err = A.MarshalBinary()
	if err != nil {
		om.metrics.InitErrorsTotal.Inc()
		return nil, nil, fmt.Errorf("ot error: %w", err)
	}

	return
}

// Transfer performs 1-out-of-N OT transfer for the given token and client point B
// returning the encrypted slots to be sent back to the client.
//
// Given N slots, group generator G, scalar a from open(token) and point B:
//
//	compute point A  = a * G
//	compute point aB = a * B
//	compute point T  = a * A
//	for each index i in [0, N):
//		compute scalar e	= new Scalar(i)
//		compute point eT   	= e * T
//		compute key 	= a*B - e*T
//		compute keyHashed	= sha256(eKey)
//		yield ciphertext	= GCM(key=keyHashed, plaintext=slot[i], nonce=RandomNonce())
func (om *OTManager) Transfer(token, pointB []byte) (ciphertexts [][]byte, err error) {
	start := time.Now()
	om.metrics.TransferCount.Inc()
	defer func() { om.metrics.TransferDurationSec.Observe(time.Since(start).Seconds()) }()

	B := Group.NewElement()
	if err := B.UnmarshalBinary(pointB); err != nil {
		om.metrics.TransferErrorsTotal.Inc()
		return nil, fmt.Errorf("ot error: %w", err)
	}
	if B.IsIdentity() {
		om.metrics.TransferErrorsTotal.Inc()
		return nil, errors.New("ot error: client point cannot be the identity of the group")
	}

	aBytes, err := om.codec.open(token)
	if err != nil {
		om.metrics.TransferErrorsTotal.Inc()
		return nil, fmt.Errorf("ot error: %w", err)
	}

	a := Group.NewScalar()
	if err := a.UnmarshalBinary(aBytes); err != nil {
		om.metrics.TransferErrorsTotal.Inc()
		return nil, fmt.Errorf("ot error: %w", err)
	}
	A := Group.NewElement().MulGen(a)

	rtx := om.backend.ReadTx()
	rtx.RLock()
	slotValue, err := rtx.UnsafeGet(schema.BucketOT, schema.KeyOTBlob)
	if err != nil {
		rtx.RUnlock()
		om.metrics.TransferErrorsTotal.Inc()
		return nil, err
	}
	rtx.RUnlock()

	slots, err := om.blobToSlots(slotValue)
	if err != nil {
		om.metrics.TransferErrorsTotal.Inc()
		return nil, err
	}

	hasher := sha256.New()
	aB := Group.NewElement().Mul(B, a)
	T := Group.NewElement().Mul(A, a)

	ciphertexts = make([][]byte, 0, om.opts.SlotCount)

	for e, slot := range slots {
		eScalar := Group.NewScalar().SetUint64(uint64(e))
		eT := Group.NewElement().Mul(T, eScalar)
		eKeyPoint := sub(aB, eT)
		eKeyPointBytes, err := eKeyPoint.MarshalBinary()
		if err != nil {
			om.metrics.TransferErrorsTotal.Inc()
			return nil, fmt.Errorf("ot error: %w", err)
		}

		// key is already 32 bytes which is enough for GCM
		// but sha256 makes it uniformly random
		hasher.Reset()
		hasher.Write(eKeyPointBytes)
		hashedKey := hasher.Sum(nil)

		encryptedSlot, err := om.encryptSlot(slot, hashedKey)
		if err != nil {
			om.metrics.TransferErrorsTotal.Inc()
			return nil, fmt.Errorf("ot error: failed to encrypt slot: %w", err)
		}
		ciphertexts = append(ciphertexts, encryptedSlot)
	}

	return ciphertexts, nil
}

func (om *OTManager) ApplyWriteAll(cmd command.CmdOTWriteAll) (*command.ResultOTWriteAll, error) {
	start := time.Now()
	om.metrics.WriteAllCount.Inc()
	defer func() { om.metrics.WriteAllDurationSec.Observe(time.Since(start).Seconds()) }()

	if err := om.CheckBlob(cmd.Blob); err != nil {
		om.metrics.WriteAllErrorsTotal.Inc()
		return nil, err
	}

	wtx := om.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()

	if err := wtx.UnsafePut(schema.BucketOT, schema.KeyOTBlob, cmd.Blob); err != nil {
		wtx.Abort()
		om.metrics.WriteAllErrorsTotal.Inc()
		return nil, fmt.Errorf("failed to write blob: %w", err)
	}

	if _, err := wtx.Commit(); err != nil {
		wtx.Abort()
		om.metrics.WriteAllErrorsTotal.Inc()
		return nil, fmt.Errorf("failed to commit blob write: %w", err)
	}

	return &command.ResultOTWriteAll{}, nil
}

func (om *OTManager) ApplyGenerateClusterKey() (*command.ResultOTGenerateClusterKey, error) {
	key, err := generateClusterKey()
	if err != nil {
		return nil, err
	}

	wtx := om.backend.WriteTx()
	wtx.Lock()
	defer wtx.Unlock()
	existingKey, err := wtx.UnsafeGet(schema.BucketOT, schema.KeyOTClusterKey)
	if err != nil {
		wtx.Abort()
		return nil, fmt.Errorf("%w: failed to read previous cluster key: %w", ErrClusterKeyGen, err)
	}
	if existingKey != nil {
		wtx.Abort()
		return nil, fmt.Errorf("%w: cluster key already exists", ErrClusterKeyGen)
	}

	if err := wtx.UnsafePut(schema.BucketOT, schema.KeyOTClusterKey, key); err != nil {
		wtx.Abort()
		return nil, fmt.Errorf("%w: %w", ErrClusterKeyGen, err)
	}
	if _, err := wtx.Commit(); err != nil {
		wtx.Abort()
		return nil, fmt.Errorf("%w: %w", ErrClusterKeyGen, err)
	}

	return &command.ResultOTGenerateClusterKey{
		Key: key,
	}, nil
}

func (om *OTManager) blobToSlots(blob []byte) (slots [][]byte, err error) {
	if err := om.CheckBlob(blob); err != nil {
		return nil, err
	}

	slotValueSize := om.opts.SlotCount * om.opts.SlotSize
	slots = make([][]byte, 0, slotValueSize)
	for i := range om.opts.SlotCount {
		start := i * om.opts.SlotSize
		end := (i + 1) * om.opts.SlotSize
		slots = append(slots, blob[start:end])
	}

	return
}

// encryptSlot encrypts the given slot with its hashed 256 bit key.
// it creates a new AES block and a new GCM, which is not cheap especially for SlotCount amount of slots
func (om *OTManager) encryptSlot(slot, key []byte) ([]byte, error) {
	if len(slot) != om.opts.SlotSize {
		panic(fmt.Sprintf("tried to encrypt slot with invalid size %d", len(slot)))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("ot error: failed to create cipher block: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("ot error: failed to create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("ot error: seal failed: %w", err)
	}

	return aead.Seal(nonce, nonce, slot, nil), nil
}

func sub(a, b group.Element) group.Element {
	negB := Group.NewElement().Neg(b)
	return Group.NewElement().Add(a, negB)
}

func (om *OTManager) CheckBlob(blob []byte) error {
	if blob == nil {
		return ErrBlobUninitialized
	}
	if len(blob) != om.opts.SlotCount*om.opts.SlotSize {
		return ErrBlobSizeCorrupted
	}

	allZeroes := true
	for _, b := range blob {
		if b != 0 {
			allZeroes = false
			break
		}
	}

	if allZeroes {
		return ErrBlobUninitialized
	}

	return nil
}

func generateClusterKey() ([]byte, error) {
	key := make([]byte, ClusterKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrClusterKeyGen, err)
	}
	return key, nil
}
