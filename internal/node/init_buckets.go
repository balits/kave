package node

import (
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage"
)

var InitBuckets = []storage.Bucket{
	kv.BucketMain,
	kv.BucketKV,
	kv.BucketLeaseWIP,
}
