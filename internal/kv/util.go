package kv

import (
	"github.com/balits/kave/internal/storage"
)

// FetchFromKeyMeta loads the key metadata from the meta bucket and returns it decoded
// if the metadata is not found, it returns nil, if an error happened during storage access or decoding, it returns the error
func FetchFromKeyMeta(store storage.Storage, key []byte) (*Meta, error) {
	metaBytes, err := store.Get(BucketKeyMeta, key)
	if err != nil {
		return nil, err
	}

	if metaBytes == nil {
		return nil, nil
	}

	meta, err := DecodeMeta(metaBytes)
	return &meta, err
}

// FetchFromKeyHistory loads the key history from the history bucket based on the composite key and returns it decoded
// if the history is not found, it returns nil, if an error happened during storage access or decoding, it returns the error
func FetchFromKeyHistory(store storage.Storage, key CompositeKey) ([]byte, error) {
	newKeyBytes, err := EncodeCompositeKey(key)
	if err != nil {
		return nil, err
	}
	valueBytes, err := store.Get(BucketKeyHistory, newKeyBytes)
	return valueBytes, nil

}

// UpdateKeyMeta encodes the metadata and updates the key meta bucket with the given key and metadata
func UpdateKeyMeta(store storage.Storage, key []byte, meta Meta) error {
	metaBytes, err := EncodeMeta(meta)
	if err != nil {
		return err
	}
	return store.Put(BucketKeyMeta, key, metaBytes)
}

// UpdateKeyHistory encodes the composite key and updates the key history bucket with the given composite key and value
func UpdateKeyHistory(store storage.Storage, key CompositeKey, value []byte) error {
	newKeyBytes, err := EncodeCompositeKey(key)
	if err != nil {
		return err
	}
	return store.Put(BucketKeyHistory, newKeyBytes, value)
}
