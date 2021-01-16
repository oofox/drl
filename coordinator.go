package main

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oofox/drl/gen"
	"google.golang.org/protobuf/types/known/durationpb"
)

func NewCoordinator() *Coordinator {
	c := &Coordinator{
		jobs:    make(chan string, 100),
		cfg:     &sync.Map{},
		ticks:   &sync.Map{},
		nodes:   &sync.Map{},
		results: &sync.Map{},
	}

	for i := 0; i < 100; i++ {
		go c.workerLoop()
	}
	return c
}

type Coordinator struct {
	gen.UnimplementedDrlServer
	cfg     *sync.Map
	jobs    chan string
	ticks   *sync.Map
	nodes   *sync.Map
	results *sync.Map
}

func (c *Coordinator) Set(key string, quota int) {
	_, ok := c.cfg.Load(key)
	if !ok {
		c.ticks.Store(key, make(chan struct{}))
		go c.runLoop(key)
	}
	c.cfg.Store(key, quota)
}

func (c *Coordinator) GetQuota(key string) int {
	v, ok := c.cfg.Load(key)
	if !ok {
		return -1
	}
	return v.(int)
}

func (c *Coordinator) runLoop(key string) {
	value, ok := c.ticks.Load(key)
	if !ok {
		return
	}
	tick := value.(chan struct{})
	timer := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-timer.C:
		case <-tick:
			timer.Reset(0)
		}
		c.jobs <- key
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
	c.nodes.Store(node.ID(), srv)
	defer c.nodes.Delete(node.ID())

	for {
		event, err := srv.Recv()
		if err != nil {
			return err
		}

		switch event.Type {
		case gen.EventType_Acquired:
			tick, ok := c.ticks.Load(event.Key)
			if !ok {
				continue
			}
			tick.(chan struct{}) <- struct{}{}
		case gen.EventType_Quota:
			v, ok := c.results.Load(event.Key)
			if !ok {
				continue
			}
			v.(*sync.Map).Store(node.ID(), event.Quota/float32(event.Duration.AsDuration()/time.Microsecond))
			c.results.Store(event.Key, v)
		}
	}
}

func (c *Coordinator) Collect(key string) {
	c.results.Store(key, &sync.Map{})

	c.nodes.Range(func(_, v interface{}) bool {
		nodeSrv := v.(gen.Drl_ConnectServer)
		_ = nodeSrv.Send(&gen.Event{
			Type: gen.EventType_Pull,
			Key:  key,
		})
		return true
	})

	time.Sleep(time.Millisecond * 10)

	keyQuota := float32(c.GetQuota(key))

	var count float32

	v, ok := c.results.Load(key)
	if !ok {
		return
	}
	result := v.(*sync.Map)

	result.Range(func(_, v interface{}) bool {
		count += v.(float32)
		return true
	})

	c.nodes.Range(func(k, v interface{}) bool {
		nodeID := k.(uint32)
		nodeSrv := v.(gen.Drl_ConnectServer)

		v, ok := result.Load(nodeID)
		if !ok {
			return true
		}
		weight := v.(float32)
		_ = nodeSrv.Send(&gen.Event{
			Type:     gen.EventType_Quota,
			Key:      key,
			Quota:    weight / count * keyQuota,
			Duration: durationpb.New(time.Millisecond * 100),
		})
		return true
	})
}
