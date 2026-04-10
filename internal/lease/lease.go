// All TTL's in this package are seconds
package lease

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/balits/kave/internal/util"
)

const maxTTL int64 = 7_889_232 // 3 months

var errDecodingFailed = fmt.Errorf("codec error: lease decoding failed")

type Lease struct {
	ID  int64 // random ID, sosem 0 (0 azt jelentené egy Entryn, hogy nics hozzárendelt Lease)
	TTL int64 // szerver által elrendelt TTL

	timeMu       sync.RWMutex // rwlock ami a lejáratot és a hátralévő ttl-t védi
	remainingTTL int64        // hátralévő TTL
	expiry       time.Time    // derived from TTL and remainingTTL

	keysMu sync.RWMutex           // rwlock ami a kulcs halmazt védi
	keySet map[LeasedKey]struct{} // kulcsok halmaza, amikhez hozzá van rendelve ez a Lease (egy kulcs - max egy lease)
}

func newLease(id, ttl int64, clock util.Clock) *Lease {
	return &Lease{
		ID:           id,
		TTL:          ttl,
		remainingTTL: ttl,
		expiry:       clock.Now().Add(time.Second * time.Duration(ttl)),
		keySet:       make(map[LeasedKey]struct{}),
	}
}

func (l *Lease) RemainingTTL() int64 {
	l.timeMu.RLock()
	defer l.timeMu.RUnlock()
	return l.remainingTTL
}

func (l *Lease) RemainingSec(clock util.Clock) int64 {
	l.timeMu.RLock()
	defer l.timeMu.RUnlock()
	return int64(clock.Until(l.expiry).Seconds())
}

func (l *Lease) Update(remTTL int64, exp time.Time) {
	l.timeMu.Lock()
	defer l.timeMu.Unlock()
	l.remainingTTL = remTTL
	l.expiry = exp
}

func (l *Lease) AttachKey(key []byte) {
	l.keysMu.Lock()
	defer l.keysMu.Unlock()
	l.keySet[LeasedKey(key)] = struct{}{}
}

func (l *Lease) DetachKey(key []byte) {
	l.keysMu.Lock()
	defer l.keysMu.Unlock()
	delete(l.keySet, LeasedKey(key))
}

func (l *Lease) KeySet() map[LeasedKey]struct{} {
	l.keysMu.RLock()
	defer l.keysMu.RUnlock()
	copy := make(map[LeasedKey]struct{}, len(l.keySet))
	for k, v := range l.keySet {
		copy[k] = v
	}
	return copy
}

type LeasedKey = string

func EncodeLease(l *Lease) ([]byte, error) {
	buf := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(buf[0:], uint64(l.ID))
	binary.BigEndian.PutUint64(buf[8:], uint64(l.TTL))
	l.timeMu.RLock()
	binary.BigEndian.PutUint64(buf[16:], uint64(l.remainingTTL))
	l.timeMu.RUnlock()
	return buf, nil
}

func DecodeLease(src []byte) (*Lease, error) {
	if len(src) < 24 {
		return nil, fmt.Errorf("%w: buffer not at least 24 bytes long", errDecodingFailed)
	}
	l := &Lease{
		ID:           int64(binary.BigEndian.Uint64(src[0:])),
		TTL:          int64(binary.BigEndian.Uint64(src[8:])),
		remainingTTL: int64(binary.BigEndian.Uint64(src[16:])),
		keySet:       make(map[LeasedKey]struct{}),
	}
	l.expiry = time.Now().Add(time.Duration(l.remainingTTL) * time.Second)
	return l, nil
}

func nextID() int64 {
	var buf [8]byte
	// it rand.Read fails, the program is crashed unrecoverably,
	// its ok to ignore, err
	_, _ = rand.Read(buf[:])
	id := int64(binary.BigEndian.Uint64(buf[:]))
	// 0 is reserved as "no lease"
	if id <= 0 {
		id = -id
	}
	return id
}
