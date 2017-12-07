package goleveldb

import (
	"encoding/binary"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brunotm/kvs"
	"github.com/brunotm/kvs/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// TODO: add key and prefix level transactions
// Must be aligned with GC

const (
	gcInterval = uint64(time.Hour)
	pathSep    = ":"
)

var (
	dopt *opt.Options
	wopt *opt.WriteOptions
	ropt *opt.ReadOptions
)

// Check if Store satisfies kvs.Store interface.
var _ kvs.Store = (*Store)(nil)

// Store leveldb
type Store struct {
	db   *leveldb.DB
	path string
	open uint32
	time uint64
	done chan struct{}
}

// New creates or open a existing Store
func New(path string) (store kvs.Store, err error) {
	if err = os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	s := &Store{}
	if s.db, err = leveldb.OpenFile(path, dopt); err != nil {
		return nil, err
	}
	s.path = path
	s.done = make(chan struct{})

	s.time = uint64(time.Now().UnixNano())
	go s.keeper()

	return s, nil
}

// Remove closes and remove the store
func (s *Store) Remove() (err error) {
	if err = s.Close(); err != nil {
		return err
	}
	return os.RemoveAll(s.path)
}

// Close the store
func (s *Store) Close() (err error) {
	if atomic.CompareAndSwapUint32(&s.open, 0, 1) {
		close(s.done)
		return s.db.Close()
	}
	return err
}

// Has returns true if the store does contains the given key
func (s *Store) Has(path ...string) (exists bool, err error) {
	_, err = s.Get(path...)
	if err == nil {
		return true, nil
	}

	if err == kvs.ErrNotFound {
		return false, nil
	}

	return false, err
}

// Set the value for the given key
func (s *Store) Set(value []byte, path ...string) (err error) {
	return s.set(strings.Join(path, pathSep), value, 0)
}

// SetWithTTL the value for the given key with a time to live
func (s *Store) SetWithTTL(value []byte, ttl time.Duration, path ...string) (err error) {
	return s.set(strings.Join(path, pathSep), value, ttl)
}

// Get the value for the given key
func (s *Store) Get(path ...string) (value []byte, err error) {
	key := []byte(strings.Join(path, pathSep))

	if value, err = s.db.Get(key, ropt); err == leveldb.ErrNotFound {
		return nil, kvs.ErrNotFound
	}

	if s.isExpired(value) {
		return nil, kvs.ErrNotFound
	}

	return value[8:], err
}

// Delete the value for the given key
func (s *Store) Delete(path ...string) (err error) {
	key := []byte(strings.Join(path, pathSep))
	return s.db.Delete(key, wopt)
}

// GetTree returns the values for keys under the given prefix
func (s *Store) GetTree(path ...string) (values [][]byte, err error) {
	prefix := []byte(strings.Join(path, pathSep))
	iter := s.getIter(prefix)
	defer iter.Release()

	for iter.Next() {
		value := iter.Value()
		if !s.isExpired(value) {
			values = append(values, utils.CopyBytes(value[8:]))
		}
	}

	return values, iter.Error()
}

// DeleteTree deletes the values for keys under the given prefix
func (s *Store) DeleteTree(path ...string) (err error) {
	prefix := []byte(strings.Join(path, pathSep))
	iter := s.getIter(prefix)
	defer iter.Release()

	batch := &leveldb.Batch{}
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	if err = iter.Error(); err != nil {
		return err
	}

	return s.db.Write(batch, wopt)
}

func (s *Store) set(key string, value []byte, ttl time.Duration) (err error) {
	return s.db.Put([]byte(key), s.encodeTTL(value, ttl), wopt)
}

func (s *Store) expire() (err error) {
	// TODO: use transaction
	batch := &leveldb.Batch{}
	iter := s.getIter(nil)
	defer iter.Release()

	for iter.Next() {
		if s.isExpired(iter.Value()) {
			batch.Delete(iter.Key())
		}
	}

	return s.db.Write(batch, wopt)
}

// getIter returns a iterator for the given prefix
func (s *Store) getIter(prefix []byte) (iter iterator.Iterator) {
	if len(prefix) > 0 {
		return s.db.NewIterator(util.BytesPrefix(prefix), nil)
	}
	return s.db.NewIterator(nil, nil)
}

func (s *Store) isExpired(value []byte) (expired bool) {
	if ttl := binary.LittleEndian.Uint64(value[:8]); ttl > 0 {
		return ttl <= atomic.LoadUint64(&s.time)
	}
	return false
}

func (s *Store) keeper() {
	var now uint64
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case t := <-ticker.C:
			// Update Store time
			now = uint64(t.UnixNano())
			atomic.StoreUint64(&s.time, now)

			// Garbage collect expired keys
			if now%gcInterval == 0 {
				go s.expire()
			}
		}
	}
}

func (s *Store) encodeTTL(value []byte, ttl time.Duration) (block []byte) {
	block = make([]byte, 8+len(value))
	if ttl > 0 {
		binary.LittleEndian.PutUint64(
			block[:8],
			atomic.LoadUint64(&s.time)+uint64(ttl.Nanoseconds()),
		)
	}

	copy(block[8:], value)
	return block
}
