package main

import (
	"time"

	"github.com/google/uuid"
	"github.com/oofox/drl/gen"
	"google.golang.org/protobuf/types/known/durationpb"
)

func NewCoordinator() *Coordinator {
	c := &Coordinator{
		cfg:     make(map[string]int),
		jobs:    make(chan string, 100),
		ticks:   make(map[string]chan struct{}),
		nodes:   make(map[uint32]gen.Drl_ConnectServer),
		results: make(map[string]map[uint32]float32),
	}

	for i := 0; i < 100; i++ {
		go c.workerLoop()
	}
	return c
}

type Coordinator struct {
	gen.UnimplementedDrlServer
	cfg     map[string]int
	jobs    chan string
	ticks   map[string]chan struct{}
	nodes   map[uint32]gen.Drl_ConnectServer
	results map[string]map[uint32]float32
}

func (c *Coordinator) Set(key string, quota int) {
	_, ok := c.cfg[key]
	if !ok {
		c.ticks[key] = make(chan struct{})
		go c.runLoop(key)
	}
	c.cfg[key] = quota
}

func (c *Coordinator) GetQuota(key string) int {
	return c.cfg[key]
}

func (c *Coordinator) runLoop(key string) {
	ticker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ticker.C:
			c.jobs <- key
		case <-c.ticks[key]:
			c.jobs <- key
			ticker.Reset(0)
		}
	}
}

func (c *Coordinator) workerLoop() {
	for {
		key := <-c.jobs
		c.Collect(key)
	}
}

func (c *Coordinator) Connect(srv gen.Drl_ConnectServer) error {
	node := uuid.New()
	c.nodes[node.ID()] = srv
	defer delete(c.nodes, node.ID())

	for {
		event, err := srv.Recv()
		if err != nil {
			return err
		}

		switch event.Type {
		case gen.EventType_Acquired:
			c.ticks[event.Key] <- struct{}{}
		case gen.EventType_Quota:
			c.results[event.Key][node.ID()] = event.Quota / float32(event.Duration.AsDuration()/time.Microsecond)
		}
	}
}

func (c *Coordinator) Collect(key string) {
	c.results[key] = make(map[uint32]float32)

	for _, nodeSrv := range c.nodes {
		err := nodeSrv.Send(&gen.Event{
			Type: gen.EventType_Pull,
			Key:  key,
		})
		if err != nil {
			continue
		}
	}
	time.Sleep(time.Millisecond * 10)

	keyQuota := float32(c.GetQuota(key))

	var count float32
	for _, weight := range c.results[key] {
		count += weight
	}

	for nodeID, nodeSrv := range c.nodes {
		if _, ok := c.results[key][nodeID]; ok {
			err := nodeSrv.Send(&gen.Event{
				Type:     gen.EventType_Quota,
				Key:      key,
				Quota:    c.results[key][nodeID] / count * keyQuota,
				Duration: durationpb.New(time.Millisecond * 100),
			})
			if err != nil {
				continue
			}
		}
	}
}
