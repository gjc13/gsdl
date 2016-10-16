package pager

import (
	"log"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

const lruCacheSize int = 16

type WritebackCallback func(filename string, pgData []byte, pgNumber uint32)

type Pager struct {
	//Cache is thread safe, map in golang can be read concurrently
	//Locked when there is need to handle evict
	//No need to lock in WritePage since only one go routine can acquire the rwlock for file in LockManager
	filename    string
	filecache   *lru.Cache
	dirtyMap    map[int]bool
	lock        sync.Mutex
	onWriteback WritebackCallback
}

type pagerManager struct {
	pagers    map[string]*Pager
	pagerRefs map[string]int
	lock      sync.Mutex
}

func (manager *pagerManager) OpenPager(string filename, onWriteback WritebackCallback) *Pager {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	pager, ok := manager.pagers[filename]
	if !ok {
		pager = &Pager{
			filename: filename,
			filecache: lru.NewWithEvict(lruCacheSize,
				func(key interface{}, value interface{}) {
					pager.onEvicted(key, value)
				}),
			onWriteback: onWriteback,
		}
		manager.pagers[filename] = pager
		manager.pagerRefs[filename] = 1
	} else {
		manager.pagerRefs[filename]++
	}
	return pager
}

func (manager *pagerManager) ClosePager(string filename) {
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
			pager.onWriteback(pager.filename, pgNumber, value.([]byte))
		}
	}
}

func (pager *Pager) ReadPage(pgNumber uint32) ([]byte, error) {
	val, ok := pager.filecache.Get(pgNumber)
	if !ok {
		pgData, err := loadPage(pager.filename, pgNumber)
		if err != nil {
			return nil, err
		}
		pager.filecache.Add(pgNumber, pgData)
		return pgData, nil
	} else {
		return val.([]byte), nil
	}
}

func (pager *Pager) WritePage(pgNumber uint32, page []byte) {
	pager.filecache.Add(pgNumber, page)
}

func (pager *Pager) SyncAllToDisk() error {
	for _, key := range pager.filecache.Keys() {
		val, _ := pager.filecache.Get(key)
		pgData := val.([]byte)
		writePageWithAppend(pager.filename, pgData, key.(uint32))
	}
}

func (pager *Pager) PurgeCache() {
	pager.filecache.Purge()
}

var pagerManagerInstance *pagerManager = nil
var once sync.Once

func getPagerManager() *pagerManager {
	once.Do(func() {
		pagerManagerInstance = &pagerManager()
	})
	return pagerManagerInstance
}
