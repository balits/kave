package storage

import "errors"

type RawKV struct {
	Key   []byte
	Value []byte
}

// A Bucket kulcsok és értékek csoportosítása a tárolóban.
// Csak induláskor jönnek létre, utána nem lehet módosítani őket.
// Ide tartozik a sima kulcs-érték bucket, de a lease bucket is, meg bármi más ami a jövőben jöhet.
type Bucket string

var (
	// a key not found logikailag nem hiba
	// a visszaadott érték egyszerűen nil lesz
	// ErrKeyNotFound    error = errors.New("key not found")

	// Hiba ha a bucket nem található
	// A bucket-ek induláskor jönnek létre, nem lehet őket módosítani
	// Szóval ezt a hibát csak úgy kaphatod meg ha rossz bucket-et adsz meg a storage függvényeknek
	ErrBucketNotFound error = errors.New("storage error: bucket not found")

	// Hiba ha a batch már le van zárva és nem lehet rajta több műveletet végezni
	ErrBatchClosed error = errors.New("storage error: batch already closed")

	// Wrapper hiba az implementáció-specifikus hibákhoz amik elő jöhetnek
	ErrInternalStorageError error = errors.New("storage error: internal storage error")

	// Hiba ha egy üres kulcsot próbálunk beszúrni
	ErrEmptyKey error = errors.New("storage error: empty key inserted")
)

// StorageKind a tároló implementáció típusa
type StorageKind string

const (
	// Memóriában tárolt, nem ír lemezre semmit.
	// Gyors, viszont a processz leállásakor minden adat elveszik.
	StorageKindInMemory StorageKind = "inmemory"

	// Tartós tár, a BoltDB könyvtárat használja.
	StorageKindBoltdb StorageKind = "boltdb"
)

type StorageOptions struct {
	// A mappa, ahol megnyílik majd az adatbázis, inmem tárolónál figyelmen kívül van hagyva
	Dir string `json:"storage_data_dir"`

	// Milyen típusú tárolót használjon
	Kind StorageKind `json:"storage_kind"`

	// Induláskor létrehozott bucket-ek listája.
	// Ha egy bucket nincs benne ebben a listában, később már nem lehet létrehozni
	InitialBuckets []Bucket
}

type CommitInfo struct {
	DeletedKeys int64
	NewKeys     int64
}
