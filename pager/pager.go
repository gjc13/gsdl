package pager

import (
	"log"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

const lruCacheSize int = 4096

type WritebackCallback func(filename string, pgData []byte, pgNumber uint32)

type Pager struct {
	//Cache is thread safe, map in golang can be read concurrently
	//Locked when there is need to handle evict
	//No need to lock in WritePage since only one go routine can acquire the rwlock for file in LockManager
	filename    string
	filecache   *lru.Cache
	dirtyMap    map[uint32]bool
	lock        sync.Mutex
	onWriteback WritebackCallback
}

type pagerManager struct {
	pagers    map[string]*Pager
	pagerRefs map[string]int
	lock      sync.Mutex
}

func (manager *pagerManager) OpenPager(filename string, onWriteback WritebackCallback) *Pager {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	pager, ok := manager.pagers[filename]
	filecache, _ := lru.NewWithEvict(lruCacheSize,
		func(key interface{}, value interface{}) {
			pager.onEvicted(key, value)
		})
	if !ok {
		pager = &Pager{
			filename:    filename,
			filecache:   filecache,
			dirtyMap:    map[uint32]bool{},
			onWriteback: onWriteback,
		}
		manager.pagers[filename] = pager
		manager.pagerRefs[filename] = 1
	} else {
		manager.pagerRefs[filename]++
	}
	return pager
}

func (manager *pagerManager) ClosePager(filename string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	_, ok := manager.pagers[filename]
	if !ok {
		log.Panicf("File %s not managed by Pager Manager", filename)
	}
	manager.pagerRefs[filename]--
	if manager.pagerRefs[filename] <= 0 {
		delete(manager.pagerRefs, filename)
		delete(manager.pagers, filename)
	}
}

func (pager *Pager) onEvicted(key interface{}, value interface{}) {
	pager.lock.Lock()
	defer pager.lock.Unlock()
	pgNumber := key.(uint32)
	dirty := pager.dirtyMap[pgNumber]
	if dirty {
		if pager.onWriteback != nil {
			pager.onWriteback(pager.filename, value.([]byte), pgNumber)
		}
	}
	delete(pager.dirtyMap, pgNumber)
}

func (pager *Pager) ReadPage(pgNumber uint32) ([]byte, error) {
	val, ok := pager.filecache.Get(pgNumber)
	if !ok {
		pgData, err := loadPage(pager.filename, pgNumber)
		if err != nil {
			return nil, err
		}
		pager.filecache.Add(pgNumber, pgData)
		pager.dirtyMap[pgNumber] = false
		return pgData, nil
	} else {
		return val.([]byte), nil
	}
}

func (pager *Pager) WritePage(pgNumber uint32, page []byte) {
	pager.filecache.Add(pgNumber, page)
	pager.dirtyMap[pgNumber] = true
}

func (pager *Pager) SyncAllToDisk() error {
	for _, key := range pager.filecache.Keys() {
		val, _ := pager.filecache.Get(key)
		pgData := val.([]byte)
		err := writePageWithAppend(pager.filename, pgData, key.(uint32))
		pager.dirtyMap[key.(uint32)] = false
		if err != nil {
			return err
		}
	}
	return nil
}

func (pager *Pager) PurgeCache() {
	pager.filecache.Purge()
	pager.dirtyMap = map[uint32]bool{}
}

var pagerManagerInstance *pagerManager = nil
var oncePagerManager sync.Once

func getPagerManager() *pagerManager {
	oncePagerManager.Do(func() {
		pagerManagerInstance = &pagerManager{
			pagers:    map[string]*Pager{},
			pagerRefs: map[string]int{},
		}
	})
	return pagerManagerInstance
}
