package kvs

import (
	"errors"
	"time"
)

var (
	// ErrNotFound key not found
	ErrNotFound = errors.New("kvs: key not found")
)

// Store interface
type Store interface {
	// NewBatch creates a new kvs.Batch
	NewBatch() (batch Batch)
	// Has returns true if the store does contains the given key
	Has(path ...string) (exists bool, err error)
	// Get the value for the given key
	Get(path ...string) (value []byte, err error)
	// GetTree returns the values for keys under the given prefix
	GetTree(path ...string) (entries []Entry, err error)
	// Set the value for the given key
	Set(value []byte, path ...string) (err error)
	// SetWithTTL the value for the given key with a time to live
	SetWithTTL(value []byte, ttl time.Duration, path ...string) (err error)
	// Delete the value for the given key
	Delete(path ...string) (err error)
	// DeleteTree deletes the values for keys under the given prefix
	DeleteTree(path ...string) (err error)
	// Events returns a channel to listen for store events
	// Events() (eventsCh <-chan Event)
	// Close the store
	Close() (err error)
	// Remove closes and remove the store
	Remove() (err error)
}

// Batch interface
type Batch interface {
	// Set the value for the given key
	Set(value []byte, path ...string)
	// SetWithTTL the value for the given key with a time to live
	SetWithTTL(value []byte, ttl time.Duration, path ...string)
	// Delete the value for the given key
	Delete(path ...string)
	// Commit writes the batch
	Write() (err error)
}

// Entry is using when reading or writing multiple entries
type Entry struct {
	Key   []byte
	Value []byte
}
