package kvs

import (
	"errors"
	"time"
)

var (
	// ErrNotFound key not found
	ErrNotFound = errors.New("kvstore: key not found")
	// ErrBadKey empty key
	ErrBadKey = errors.New("kvstore: bad value")
)

// Store interface
type Store interface {
	// Has returns true if the store does contains the given key
	Has(key string) (exists bool, err error)
	// Get the value for the given key
	Get(key string) (value []byte, err error)
	// GetTree returns the values for keys under the given prefix
	GetTree(prefix string) (values [][]byte, err error)
	// Set the value for the given key
	Set(key string, value []byte) (err error)
	// SetWithTTL the value for the given key with a time to live
	SetWithTTL(key string, value []byte, ttl time.Duration) (err error)
	// Delete the value for the given key
	Delete(key string) (err error)
	// DeleteTree deletes the values for keys under the given prefix
	DeleteTree(prefix string) (err error)
	// Events returns a channel to listen for store events
	// Events() (eventsCh <-chan Event)
	// Close the store
	Close() (err error)
	// Remove closes and remove the store
	Remove() (err error)
}
