package redis

import (
	"errors"
	"sync"

	"../avltree"
)

type (
	Redis struct {
		keyBasedCache   *avltree.PairTree
		keysMutex       *sync.RWMutex
		valueBasedCache *avltree.PairTree
		valuesMutex     *sync.RWMutex
	}

	CacheItem struct {
		Key   string
		Value string
	}

	ValueCount struct {
		Value string
		Count int
	}

	LogItem struct {
		Command string
		CacheItem
	}
)

var (
	NotFoundError = errors.New("NULL")
)

func NewRedis() *Redis {
	return &Redis{
		keyBasedCache:   avltree.NewPairTree(0),
		keysMutex:       &sync.RWMutex{},
		valueBasedCache: avltree.NewPairTree(0),
		valuesMutex:     &sync.RWMutex{},
	}
}

func (r *Redis) Get(key string, resp *CacheItem) error {
	r.keysMutex.Lock()
	defer r.keysMutex.Unlock()

	v := r.keyBasedCache.Find(key)

	if v == nil {
		return NotFoundError
	}

	ptr := v.Value.(*string)
	*resp = CacheItem{key, *ptr}

	return nil
}

func (r *Redis) Set(item *CacheItem, ack *bool) error {
	*ack = false
	r.keysMutex.Lock()
	defer r.keysMutex.Unlock()

	v := r.keyBasedCache.Find(item.Key)
	if v == nil {
		r.keyBasedCache.Add(avltree.Pair{item.Key, &(item.Value)})
	} else {
		r.incCountForValue(*(v.Value.(*string)), -1)
		*(v.Value.(*string)) = item.Value
	}

	r.incCountForValue(item.Value, 1)

	*ack = true

	return nil
}

func (r *Redis) Unset(key string, ack *bool) error {
	*ack = false
	r.keysMutex.Lock()
	defer r.keysMutex.Unlock()

	v := r.keyBasedCache.Find(key)
	if v == nil {
		return NotFoundError
	}

	r.keyBasedCache.Remove(key)

	r.incCountForValue(*(v.Value.(*string)), -1)

	*ack = true

	return nil
}

func (r *Redis) GetCount(value string, count *int) error {
	*count = 0
	r.valuesMutex.Lock()
	defer r.valuesMutex.Unlock()

	v := r.valueBasedCache.Find(value)

	if v == nil {
		return nil
	}

	*count = *(v.Value.(*int))
	return nil
}

func (r *Redis) ExecuteLog(log []LogItem, ack *bool) error {
	*ack = false
	for _, item := range log {
		if item.Command == "SET" {
			if err := r.Set(&item.CacheItem, ack); err != nil {
				return err
			}
		} else {
			if err := r.Unset(item.Key, ack); err != nil {
				return err
			}
		}
	}
	*ack = true
	return nil
}

func (r *Redis) incCountForValue(value string, count int) {
	r.valuesMutex.Lock()
	defer r.valuesMutex.Unlock()

	countNode := r.valueBasedCache.Find(value)
	if countNode == nil {
		r.valueBasedCache.Add(avltree.Pair{value, &count})
	} else {
		newCount := *(countNode.Value.(*int)) + count
		*(countNode.Value.(*int)) = newCount
	}
}
