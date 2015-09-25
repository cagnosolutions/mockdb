package mockdb

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

type MockDB struct {
	FilePath string
	Stores   map[string]*map[string]interface{}
	Update   bool
	sync.RWMutex
}

func NewMockDB(filepath string, rate int64) *MockDB {
	db := &MockDB{
		FilePath: filepath,
		Stores:   make(map[string]*map[string]interface{}),
	}
	db.Lock()
	db.Load()
	db.Unlock()
	db.catchSigInt()
	go db.savesnapshots(rate)
	return db
}

func (db *MockDB) catchSigInt() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGSTOP)
	go func() {
		fmt.Printf("\nCaught %v\n", <-sig)
		db.Save()
		os.Exit(0)
	}()
}

func fillPtr(val, ptr interface{}) bool {
	b, err := json.Marshal(val)
	if err != nil {
		return false
	}
	if err := json.Unmarshal(b, &ptr); err != nil {
		return false
	}
	return true
}

func (db *MockDB) QueryAll(key string, query map[string]interface{}, ptr interface{}) (int, bool) {
	return 0, false
}

func (db *MockDB) Query(key string, query map[string]interface{}, ptr interface{}) bool {
	db.RLock()
	defer db.RUnlock()
	// get store, return false if not exists
	store, ok := db.Stores[key]
	if !ok {
		return false
	}
	// attempt to marshal query map into json
	var qry map[string]interface{}
	if b, err := json.Marshal(query); err == nil {
		if err := json.Unmarshal(b, &qry); err != nil {
			log.Fatal(err)
		}
	}
	var qryCount int
	// find internal map in this store
	for _, value := range *store {
		// reset query count for every record/row
		qryCount = len(query)
		// if we found the internal map...
		if reflect.TypeOf(value).Kind() == reflect.Map {
			// range the value map first (type asserting it accordingly)...
			for recK, recV := range value.(map[string]interface{}) { // iterate "columns" of a single record/row
				// now range the (hopefully smaller) json converted qry map second...
				for qryK, qryV := range qry {
					// check for a match
					if qryK == recK && qryV == recV {
						// if match was found, decrement counter
						qryCount--
						// if counter is zero...
						if qryCount == 0 {
							// marshal into pointer and return true
							if b, err := json.Marshal(qry); err == nil {
								if err := json.Unmarshal(b, &ptr); err != nil {
									log.Fatal(err)
								}
							}
							return true
						}
					} // otherwise keep looping until all queries are finished or matched...
				}
			}
		}
	}
	return false
}

func (db *MockDB) GetStore(key string) *map[string]interface{} {
	db.RLock()
	if store, ok := db.Stores[key]; ok {
		db.RUnlock()
		return store
	}
	db.RUnlock()
	db.Lock()
	db.Stores[key] = &map[string]interface{}{}
	store := db.Stores[key]
	db.Unlock()
	return store
}

func (db *MockDB) Add(key string, val interface{}) string {
	store, uuid := db.GetStore(key), UUID4()
	db.Lock()
	(*store)[uuid] = val
	db.Update = true
	db.Unlock()
	return uuid
}

func (db *MockDB) Set(key, fld string, val interface{}) {
	store := db.GetStore(key)
	db.Lock()
	(*store)[fld] = toMap(val)
	db.Update = true
	db.Unlock()
}

func (db *MockDB) GetAllStores(key string) map[string]*map[string]interface{} {
	db.RLock()
	defer db.RUnlock()
	return db.Stores
}

func (db *MockDB) Get(key, fld string) interface{} {
	store := db.GetStore(key)
	db.RLock()
	if val, ok := (*store)[fld]; ok {
		db.RUnlock()
		return val
	}
	db.RUnlock()
	return nil
}
func (db *MockDB) GetAs(key, fld string, ptr interface{}) bool {
	store := db.GetStore(key)
	db.RLock()
	defer db.RUnlock()
	if val, ok := (*store)[fld]; ok {
		b, err := json.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(b, &ptr); err != nil {
			log.Fatal(err)
		}
		return true
	}
	return false
}

func (db *MockDB) Del(key, fld string) {
	store := db.GetStore(key)
	db.Lock()
	delete(*store, fld)
	db.Update = true
	db.Unlock()
}

func (db *MockDB) DelStore(key string) {
	db.Lock()
	delete(db.Stores, key)
	db.Update = true
	db.Unlock()
}

func (db *MockDB) savesnapshots(rate int64) {
	time.AfterFunc(time.Duration(rate)*time.Second, func() {
		if db.Update {
			db.Lock()
			db.Save()
			db.Update = false
			db.Unlock()
		}
		db.savesnapshots(rate)
	})
}

func (db *MockDB) Save() {
	fd, err := os.Create(db.FilePath + ".tmp")
	if err != nil {
		log.Fatal(err)
	}
	json.NewEncoder(fd).Encode(db.Stores)
	if err := fd.Sync(); err != nil {
		log.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(db.FilePath+".tmp", db.FilePath); err != nil {
		os.Remove(db.FilePath + ".tmp")
		log.Fatal(err)
	}
	debug.FreeOSMemory()
}

func (db *MockDB) Load() {
	if _, err := os.Stat(db.FilePath); os.IsNotExist(err) {
		_, err := os.Create(db.FilePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	fd, err := os.Open(db.FilePath)
	if err != nil {
		fd, err = os.Create(db.FilePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	json.NewDecoder(fd).Decode(&db.Stores)
	if err := fd.Close(); err != nil {
		log.Fatal(err)
	}
	debug.FreeOSMemory()
}

func UUID4() string {
	u := make([]byte, 16)
	if _, err := rand.Read(u[:16]); err != nil {
		log.Println(err)
	}
	u[8] = (u[8] | 0x80) & 0xbf
	u[6] = (u[6] | 0x40) & 0x4f
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func toMap(v interface{}) map[string]interface{} {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(b, &m)
	if err != nil {
		log.Fatal(err)
	}
	return m
}
