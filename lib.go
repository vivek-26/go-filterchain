package filterchain

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

// Executer ...
type Executer interface {
	Execute(*Chain, *Store) error
}

// Store ...
type Store struct {
	// guards data
	sync.RWMutex

	// data store for filters in a chain
	data map[string]interface{}
}

// Put adds key/value pair to store
func (s *Store) Put(key string, value interface{}) {
	s.Lock()
	defer s.Unlock()
	s.data[key] = value
}

// Get fetches value for the given key from store
func (s *Store) Get(key string) (interface{}, bool) {
	s.RLock()
	defer s.RUnlock()
	var value, ok = s.data[key]
	return value, ok
}

// serialFilter executes sequentially
type serialFilter struct {
	filter Executer
}

func (sf *serialFilter) Execute(chain *Chain, store *Store) error {
	var err error
	if err = sf.filter.Execute(chain, store); err != nil {
		return err
	}
	return nil
}

// parallelFilter executes concurrently
type parallelFilter struct {
	done    bool
	filters []Executer
}

func (pf *parallelFilter) Execute(chain *Chain, store *Store) error {
	var g errgroup.Group
	for _, filter := range pf.filters {
		var filter = filter
		g.Go(func() error {
			var err error
			if err = filter.Execute(chain, store); err != nil {
				return err
			}
			return nil
		})
	}

	var err error
	if err = g.Wait(); err != nil {
		return err
	}

	pf.done = true
	return chain.Next(store)
}

// Chain ...
type Chain struct {
	pos     int
	filters []Executer
}

// New creates a new chain & data store
func New() (*Chain, *Store) {
	return &Chain{pos: 0, filters: make([]Executer, 0)}, &Store{data: make(map[string]interface{})}
}

// AddSerialFilter ...
func (chain *Chain) AddSerialFilter(filter Executer) *Chain {
	var sf = &serialFilter{filter: filter}
	chain.filters = append(chain.filters, sf)
	return chain
}

// AddParallelFilter ...
func (chain *Chain) AddParallelFilter(filters ...Executer) *Chain {
	switch len(filters) {
	case 0:
		return chain
	case 1:
		return chain.AddSerialFilter(filters[0])
	default:
		var pf = &parallelFilter{filters: filters, done: false}
		chain.filters = append(chain.filters, pf)
		return chain
	}
}

// Execute starts executing filters in the chain.
func (chain *Chain) Execute(store *Store) error {
	var pos = chain.pos
	if pos < len(chain.filters) {
		chain.pos++
		if err := chain.filters[pos].Execute(chain, store); err != nil {
			return err
		}
	}

	return nil
}

// Next executes the next filter in the chain.
func (chain *Chain) Next(store *Store) error {
	var pos = chain.pos - 1
	switch filter := chain.filters[pos].(type) {
	case *parallelFilter:
		if filter.done {
			return chain.Execute(store)
		}
		return nil // one or more filters are remaining to be processed
	default:
		return chain.Execute(store)
	}
}
