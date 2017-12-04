package goleveldb

import (
	"strings"
	"time"

	"github.com/brunotm/kvs"

	"github.com/syndtr/goleveldb/leveldb"
)

type Batch struct {
	store *Store
	batch *leveldb.Batch
}

// NewBatch creates a new Batch
func (s *Store) NewBatch() (batch kvs.Batch) {
	b := &Batch{}
	b.store = s
	b.batch = &leveldb.Batch{}
	return b
}

// Write batch
func (b *Batch) Write() (err error) {
	defer b.batch.Reset()
	return b.store.db.Write(b.batch, wopt)
}

// Delete the value for the given key
func (b *Batch) Delete(path ...string) {
	b.batch.Delete([]byte(strings.Join(path, pathSep)))
}

// Set the value for the given key
func (b *Batch) Set(value []byte, path ...string) {
	b.set(strings.Join(path, pathSep), value, 0)
}

// SetWithTTL the value for the given key with a time to live
func (b *Batch) SetWithTTL(value []byte, ttl time.Duration, path ...string) {
	b.set(strings.Join(path, pathSep), value, ttl)
}

func (b *Batch) set(key string, value []byte, ttl time.Duration) {
	b.batch.Put([]byte(key), b.store.encodeTTL(value, ttl))
}
