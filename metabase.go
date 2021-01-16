package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/faceair/request"
)

type Database struct {
	ID     int `json:"id"`
	DbName struct {
		O string `json:"O"`
	} `json:"db_name"`
}

type Table struct {
	ID   int `json:"id"`
	Name struct {
		O string `json:"O"`
	} `json:"name"`
}

func NewMetaBase(baseURL string) *MetaBase {
	meta := &MetaBase{
		req: request.New().SetBaseURL(baseURL),
	}
	go meta.runUpdateLoop()
	return meta
}

type MetaBase struct {
	mux      sync.RWMutex
	req      *request.Request
	database map[string]map[string]int
}

func (m *MetaBase) runUpdateLoop() {
	ctx := context.TODO()

	ticker := time.NewTicker(time.Minute)
	for ; true; <-ticker.C {
		err := m.update(ctx)
		if err != nil {
			log.Printf("update failed %s", err)
		}
	}
}

func (m *MetaBase) update(ctx context.Context) error {
	resp, err := m.req.Get(ctx, "/schema")
	if err != nil {
		return err
	}

	dbs := make([]Database, 0)
	err = resp.ToJSON(&dbs)
	if err != nil {
		return err
	}

	database := make(map[string]map[string]int)
	for _, db := range dbs {
		resp, err = m.req.Get(ctx, fmt.Sprintf("/schema/%s", db.DbName.O))
		if err != nil {
			return err
		}
		tables := make(map[string]int)
		tbs := make([]Table, 0)
		for _, tb := range tbs {
			tables[tb.Name.O] = tb.ID
		}
		database[db.DbName.O] = tables
	}

	m.mux.Lock()
	m.database = database
	m.mux.Unlock()

	return nil
}

func (m *MetaBase) Databases() []string {
	m.mux.RLock()
	defer m.mux.RUnlock()

	databases := make([]string, 0)
	for database := range m.database {
		databases = append(databases, database)
	}
	return databases
}

func (m *MetaBase) Tables(database string) map[string]int {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.database[database]
}
