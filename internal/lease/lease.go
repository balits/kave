// All TTL's in this package are seconds
package lease

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"
)

const maxTTL int64 = 7_889_232 // 3 months

type Lease struct {
	ID           int64                  // random ID, sosem 0 (0 azt jelentené egy Entryn, hogy nics hozzárendelt Lease)
	ttl          int64                  // szerver által elrendelt TTL
	remainingTTL int64                  // hátralévő TTL
	expiryMu     sync.RWMutex           // rwlock ami a lejáratot védi
	expiry       time.Time              // derived from TTL and remainingTTL
	keysMu       sync.RWMutex           // rwlock ami a kulcs halmazt védi
	keys         map[LeasedKey]struct{} // kulcsok halmaza, amikhez hozzá van rendelve ez a Lease (egy kulcs - max egy lease)
}

func (l *Lease) AttachKey(key LeasedKey) {
	l.keysMu.Lock()
	defer l.keysMu.Unlock()
	l.keys[key] = struct{}{}
}

func (l *Lease) DetachKey(key LeasedKey) {
	l.keysMu.Lock()
	defer l.keysMu.Unlock()
	delete(l.keys, key)
}

func (l *Lease) Expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeasedKey struct {
	Key string
}

func EncodeLease(l *Lease) ([]byte, error) {
	buf := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(buf[0:], uint64(l.ID))
	binary.BigEndian.PutUint64(buf[8:], uint64(l.ttl))
	binary.BigEndian.PutUint64(buf[16:], uint64(l.remainingTTL))
	return buf, nil
}

var errDecodingFailed = fmt.Errorf("codec error: lease decoding failed")

func DecodeLease(src []byte) (*Lease, error) {
	if len(src) < 24 {
		return nil, fmt.Errorf("%w: buffer not at least 24 bytes long", errDecodingFailed)
	}
	l := &Lease{
		ID:           int64(binary.BigEndian.Uint64(src[0:])),
		ttl:          int64(binary.BigEndian.Uint64(src[8:])),
		remainingTTL: int64(binary.BigEndian.Uint64(src[16:])),
		keys:         make(map[LeasedKey]struct{}),
	}
	l.expiry = time.Now().Add(time.Duration(l.remainingTTL) * time.Second)
	return l, nil
}
