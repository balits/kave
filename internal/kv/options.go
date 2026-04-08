package kv

import "errors"

var (
	errOptionMaxKeySizeLEZero = errors.New("max key size cannot be negative or zero")
	errOptionMaxValSizeEZero  = errors.New("max value size cannot be negative or zero")
	errKeyTooLarge            = errors.New("key size exceeds max key size")
	errValueTooLarge          = errors.New("value size exceeds max key size")
)

type Options struct {
	MaxKeySize   int `json:"kv_max_key_size"`
	MaxValueSize int `json:"kv_max_value_size"`
}

func (o *Options) Check() error {
	if o.MaxKeySize < 1 {
		return errOptionMaxKeySizeLEZero
	}
	if o.MaxValueSize < 1 {
		return errOptionMaxValSizeEZero
	}
	return nil
}

func (o *Options) CheckKey(key []byte) error {
	if o.MaxKeySize < len(key) {
		return errKeyTooLarge
	}
	return nil
}

func (o *Options) CheckValue(val []byte) error {
	if o.MaxValueSize < len(val) {
		return errValueTooLarge
	}
	return nil
}
