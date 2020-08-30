package filterchain

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteToStore(t *testing.T) {
	var store = &Store{data: make(map[string]interface{})}
	store.Put("Key", "XA1")
	assert.Len(t, store.data, 1)

	var _, ok = store.data["Key"]
	assert.True(t, ok)
}

func TestFetchFromStore(t *testing.T) {
	var store = &Store{data: make(map[string]interface{})}
	store.Put("Key", "XA1")

	var iValue, ok = store.Get("Key")
	assert.True(t, ok)

	var stringValue string
	stringValue, ok = iValue.(string)
	assert.True(t, ok)
	assert.Equal(t, stringValue, "XA1")
}

func TestStoreConcurrentWrites(t *testing.T) {
	var concurrentWrites = func() {
		var store = &Store{data: make(map[string]interface{})}
		var wg sync.WaitGroup

		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var key = fmt.Sprintf("Key-%d", i)
				store.Put(key, i)
			}(i)
		}

		wg.Wait()
	}
	assert.NotPanics(t, concurrentWrites)
}

func TestCreateNewChain(t *testing.T) {
	var chain, store = New(nil)
	assert.NotNil(t, chain.Ctx)
	assert.Equal(t, 0, chain.pos)
	assert.Len(t, chain.filters, 0)
	assert.Len(t, store.data, 0)
}

func TestNewFilter(t *testing.T) {
	var result = 1
	var filter = NewFilter(func(chain *Chain, store *Store) error {
		result++
		return nil
	})

	var inlineFilterType *inline
	assert.IsType(t, inlineFilterType, filter)

	filter.Execute(nil, nil)
	assert.Equal(t, 2, result)
}

func TestAddFilters(t *testing.T) {
	var chain, _ = New(nil)
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		return nil
	})
	var filter2 = NewFilter(func(chain *Chain, store *Store) error {
		return nil
	})

	chain.AddFilters(filter1, filter2)
	assert.Len(t, chain.filters, 2)

	var serialFilterType *serialFilter
	assert.IsType(t, serialFilterType, chain.filters[0])
	assert.IsType(t, serialFilterType, chain.filters[1])
}

func TestAddParallelFilters(t *testing.T) {
	var chain, _ = New(nil)
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		return nil
	})
	var filter2 = NewFilter(func(chain *Chain, store *Store) error {
		return nil
	})
	var filter3 = NewFilter(func(chain *Chain, store *Store) error {
		return nil
	})

	chain.AddParallelFilters()
	chain.AddParallelFilters(filter1)
	chain.AddParallelFilters(filter2, filter3)

	var serialFilterType *serialFilter
	var parallelFilterType *parallelFilter
	assert.Len(t, chain.filters, 2)
	assert.IsType(t, serialFilterType, chain.filters[0])
	assert.IsType(t, parallelFilterType, chain.filters[1])
}

func TestSerialFilter(t *testing.T) {
	var chain, store = New(nil)
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		store.Put("Key", 1)
		return nil
	})

	chain.AddFilters(filter1)
	assert.Len(t, chain.filters, 1)
	var serialFilter, ok = chain.filters[0].(*serialFilter)
	assert.True(t, ok)
	var err = serialFilter.Execute(chain, store)
	assert.NoError(t, err)
	var result int
	var resultInterface interface{}
	resultInterface, ok = store.Get("Key")
	assert.True(t, ok)
	result, ok = resultInterface.(int)
	assert.True(t, ok)
	assert.Equal(t, result, 1)
}

func TestSerialFilterError(t *testing.T) {
	var chain, _ = New(nil)
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		return errors.New("filter execution failed")
	})

	chain.AddFilters(filter1)
	assert.Len(t, chain.filters, 1)
	var serialFilter, ok = chain.filters[0].(*serialFilter)
	assert.True(t, ok)
	var err = serialFilter.Execute(chain, nil)
	assert.EqualError(t, err, "filter execution failed")
}

func TestParallelFilter(t *testing.T) {
	var chain, _ = New(nil)
	var result = new(uint64)
	*result = 0
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		atomic.AddUint64(result, uint64(1))
		return chain.Next(nil)
	})
	var filter2 = NewFilter(func(chain *Chain, store *Store) error {
		atomic.AddUint64(result, uint64(1))
		return chain.Next(nil)
	})
	var filter3 = NewFilter(func(chain *Chain, store *Store) error {
		atomic.AddUint64(result, uint64(1))
		return chain.Next(nil)
	})

	chain.AddParallelFilters(filter1, filter2, filter3)
	assert.Len(t, chain.filters, 1)
	chain.pos++ // signal chain that it's processing first filter
	var parallelFilter, ok = chain.filters[0].(*parallelFilter)
	assert.True(t, ok)
	var err = parallelFilter.Execute(chain, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), *result)
}

func TestParallelFilterError(t *testing.T) {
	var chain, _ = New(nil)
	var filter1 = NewFilter(func(chain *Chain, store *Store) error {
		return chain.Next(nil)
	})
	var filter2 = NewFilter(func(chain *Chain, store *Store) error {
		return errors.New("filter execution failed")
	})
	var filter3 = NewFilter(func(chain *Chain, store *Store) error {
		return chain.Next(nil)
	})

	chain.AddParallelFilters(filter1, filter2, filter3)
	assert.Len(t, chain.filters, 1)
	chain.pos++ // signal chain that it's processing first filter
	var parallelFilter, ok = chain.filters[0].(*parallelFilter)
	assert.True(t, ok)
	var err = parallelFilter.Execute(chain, nil)
	assert.EqualError(t, err, "filter execution failed")
}
