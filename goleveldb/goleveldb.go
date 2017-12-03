package goleveldb

import (
	"encoding/binary"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brunotm/kvs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Check if Store satisfies kvs.Store interface.
var _ kvs.Store = (*Store)(nil)

const (
	gcInterval = uint64(time.Hour)
	pathSep    = ":"
)

var (
	dopt *opt.Options
	wopt *opt.WriteOptions
	ropt *opt.ReadOptions
)

// Store leveldb
type Store struct {
	db   *leveldb.DB
	path string
	open uint32
	time uint64
	done chan struct{}
}

// New creates or open a existing goleveldb Store
func New(path string) (store *Store, err error) {
	if err = os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	store = &Store{}
	if store.db, err = leveldb.OpenFile(path, dopt); err != nil {
		return nil, err
	}
	store.path = path
	store.done = make(chan struct{})

	store.time = uint64(time.Now().UnixNano())
	go store.keeper()

	return store, nil
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
	key := strings.Join(path, pathSep)
	if key == "" {
		return nil, kvs.ErrBadKey
	}

	if value, err = s.db.Get([]byte(key), ropt); err == leveldb.ErrNotFound {
		return nil, kvs.ErrNotFound
	}

	if s.isExpired(value) {
		return nil, kvs.ErrNotFound
	}

	return value[8:], err
}

// Delete the value for the given key
func (s *Store) Delete(path ...string) (err error) {
	key := strings.Join(path, pathSep)
	return s.db.Delete([]byte(key), wopt)
}

// GetTree returns the values for keys under the given prefix
func (s *Store) GetTree(path ...string) (values [][]byte, err error) {
	prefix := strings.Join(path, pathSep)

	iter := s.getIter([]byte(prefix))
	defer iter.Release()

	for iter.Next() {
		value := iter.Value()
		if !s.isExpired(value) {
			values = append(values, copyBytes(value[8:]))
		}
	}

	return values, iter.Error()
}

// DeleteTree deletes the values for keys under the given prefix
func (s *Store) DeleteTree(path ...string) (err error) {
	prefix := strings.Join(path, pathSep)

	batch := &leveldb.Batch{}
	iter := s.getIter([]byte(prefix))
	defer iter.Release()

	for iter.Next() {
		batch.Delete(iter.Key())
	}

	if err = iter.Error(); err != nil {
		return err
	}

	return s.db.Write(batch, wopt)
}

func (s *Store) set(key string, value []byte, ttl time.Duration) (err error) {
	if key == "" {
		return kvs.ErrBadKey
	}

	buf := make([]byte, 8+len(value))
	if ttl > 0 {
		binary.LittleEndian.PutUint64(
			buf[:8],
			atomic.LoadUint64(&s.time)+uint64(ttl.Nanoseconds()),
		)
	}

	copy(buf[8:], value)
	return s.db.Put([]byte(key), buf, wopt)
}

func (s *Store) expire() (err error) {
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
		return ttl < atomic.LoadUint64(&s.time)
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

// Converts bytes to an uint
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func copyBytes(b []byte) (c []byte) {
	c = make([]byte, len(b))
	copy(c, b)
	return c
}
