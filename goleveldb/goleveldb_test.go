package goleveldb

import (
	"strconv"
	"testing"
	"time"

	"github.com/brunotm/kvs"
	"github.com/matryer/is"
)

const (
	testPath = "teststore"
)

var (
	testKey   = "testKey"
	testValue = []byte(`testValue`)
)

func TestNewCloseRemove(t *testing.T) {
	is := is.New(t)

	// Create a new store
	store, err := New(testPath)
	is.NoErr(err)

	// Try to create a new store with the same path, expect EAGAIN
	_, err = New(testPath)
	is.True(err != nil)

	// Store cannot be nil
	is.True(store != nil)

	// Close store
	is.NoErr(store.Close())

	// Remove a closed store
	is.NoErr(store.Remove())
}

func TestCRUD(t *testing.T) {
	is := is.New(t)

	// Create a new store
	store, err := New(testPath)
	is.NoErr(err)

	// Set a key/value
	err = store.Set(testValue, testKey)
	is.NoErr(err)

	// Check if key exists
	exists, err := store.Has(testKey)
	is.NoErr(err)
	is.True(exists == true)

	// Get and compare previous key/value
	value, err := store.Get(testKey)
	is.NoErr(err)
	is.True(string(value) == string(testValue))

	// Delete previous key/value
	err = store.Delete(testKey)
	is.NoErr(err)

	// Check key/value if exists
	exists, err = store.Has(testKey)
	is.NoErr(err)
	is.True(exists == false)

	// Try set with empty key
	err = store.Set(testValue, "")
	is.True(err == kvs.ErrBadKey)

	// Try get with empty key
	_, err = store.Get("")
	is.True(err == kvs.ErrBadKey)

	is.NoErr(store.Remove())
}

func TestCRUDTTL(t *testing.T) {
	is := is.New(t)

	// Create a new store
	store, err := New(testPath)
	is.NoErr(err)

	// Set a key/value with ttl
	err = store.SetWithTTL(testValue, time.Millisecond*2, testKey)
	is.NoErr(err)

	// Check if key exists
	exists, err := store.Has(testKey)
	is.NoErr(err)
	is.True(exists == true)

	// Sleep beyond ttl
	time.Sleep(time.Millisecond * 3)

	// Check if expired key exists
	exists, err = store.Has(testKey)
	is.NoErr(err)
	is.True(exists == false)

	// Set a key/value with ttl
	err = store.SetWithTTL(testValue, time.Millisecond*2, testKey)
	is.NoErr(err)

	// Get key/value
	value, err := store.Get(testKey)
	is.NoErr(err)
	is.True(string(value) == string(testValue))

	// Sleep beyond ttl
	time.Sleep(time.Millisecond * 3)

	// Expire
	err = store.expire()
	is.NoErr(err)

	// Check if key exists within leveldb
	exists, err = store.db.Has([]byte(`testKey`), ropt)
	is.NoErr(err)
	is.True(exists == false)

	is.NoErr(store.Remove())
}

func TestCRUDTree(t *testing.T) {
	is := is.New(t)

	// Create a new store
	store, err := New(testPath)
	is.NoErr(err)

	// Set key/values
	for x := 0; x < 10; x++ {
		err = store.Set(testValue, testKey+strconv.Itoa(x))
		is.NoErr(err)
	}

	// Check if keys exists
	for x := 0; x < 10; x++ {
		exists, err := store.Has(testKey + strconv.Itoa(x))
		is.NoErr(err)
		is.True(exists == true)
	}

	// Get keys
	values, err := store.GetTree(testKey)
	is.NoErr(err)
	is.True(len(values) == 10)

	// Delete previous key/value
	err = store.DeleteTree(testKey)
	is.NoErr(err)

	// Check key/value if exists
	exists, err := store.Has(testKey)
	is.NoErr(err)
	is.True(exists == false)

	is.NoErr(store.Remove())
}

func BenchmarkInsertGet(b *testing.B) {
	store, err := New(testPath)
	defer store.Remove()

	if err != nil {
		panic(err.Error())
	}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for x := 0; x < 100; x++ {
			k := testKey + strconv.Itoa(x)
			err := store.Set(testValue, k)
			if err != nil {
				panic(err)
			}
			_, err = store.Get(k)
			if err != nil {
				panic(err)
			}
		}
	}
}
