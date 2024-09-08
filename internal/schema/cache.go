package schema

import (
	"sync"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

type Cache struct {
	mu            sync.RWMutex
	current       *Schema
	cycleCount    int
	refreshEveryN int
}

func NewCache(refreshEveryN int) *Cache {
	return &Cache{
		refreshEveryN: refreshEveryN,
	}
}

func (c *Cache) Get(logs []datadogV2.Log) Schema {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cycleCount++

	if c.current == nil || c.cycleCount >= c.refreshEveryN {
		s := Discover(logs)
		c.current = &s
		c.cycleCount = 0
	}

	return *c.current
}

func (c *Cache) Current() *Schema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

func (c *Cache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = nil
	c.cycleCount = 0
}
