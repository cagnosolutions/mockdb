package mockdb

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	db.Load()
	go db.savesnapshots(rate)
	db.catchSigInt()
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
	(*store)[fld] = val
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
	log.Println("Savesnapshot iterating...")
	time.AfterFunc(time.Duration(rate)*time.Second, func() {
		if db.Update {
			log.Println("Saving snapshot...")
			db.Lock()
			db.Save()
			db.Unlock()
		}
		db.savesnapshots(rate)
	})
}

func (db *MockDB) Save() {
	log.Println("Saving data to drive...")
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
	log.Println("Finished saving.")
}

func (db *MockDB) Load() {
	log.Println("Loading data off drive...")
	if _, err := os.Stat(db.FilePath); os.IsNotExist(err) {
		log.Printf("%q does not exists, attempting to create...\n", db.FilePath)
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
	log.Println("Finished loading.")
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
