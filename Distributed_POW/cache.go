package distpow

import (
	"bytes"
	"encoding/gob"
	"github.com/DistributedClocks/tracing"
	"log"
	"sync"
)

type CacheAdd struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheRemove struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheHit struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheMiss struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CacheValue struct {
	workerByte   uint8
	secret		 []uint8
	numTrailingZeros uint
}

type CacheBox struct {
	cacheBox	  map[string]CacheValue
	mu 			  sync.Mutex
	muRemoveAdd   sync.Mutex
	muAddSafe	  sync.Mutex
}

func NewCacheBox() *CacheBox {
	box := make(map[string]CacheValue)

	return &CacheBox{
		cacheBox: box,
	}
}

func (c *CacheBox) remove(nonce []uint8, numTrailingZeros uint, trace *tracing.Trace)  {
	c.muRemoveAdd.Lock()
	cacheKey := c.CacheEncoder(nonce)
	cacheVal := c.cacheBox[cacheKey]
	delete(c.cacheBox, cacheKey)
	trace.RecordAction(CacheRemove{
		Nonce: nonce,
		NumTrailingZeros: cacheVal.numTrailingZeros,
		Secret: generateByteSecret(cacheVal.workerByte, cacheVal.secret),
	})
	c.muRemoveAdd.Unlock()
}

func (c *CacheBox) addSafe(nonce []uint8, numTrailingZeros uint, workerByte uint8, secret []uint8, trace *tracing.Trace) bool {
	c.muRemoveAdd.Lock()
	c.muAddSafe.Lock()
	cacheKey := c.CacheEncoder(nonce)
	addValue := CacheValue{
		secret: secret,
		numTrailingZeros: numTrailingZeros,
		workerByte: workerByte,
	}
	c.cacheBox[cacheKey] = addValue
	trace.RecordAction(CacheAdd{
		Nonce: nonce,
		NumTrailingZeros: numTrailingZeros,
		Secret: generateByteSecret(addValue.workerByte, addValue.secret),
	})
	c.muRemoveAdd.Unlock()
	c.muAddSafe.Unlock()
	return true
}

func (c *CacheBox) add(nonce []uint8, numTrailingZeros uint, workerByte uint8, secret []uint8, trace *tracing.Trace) bool {
	cacheByte, cacheVal, hitZeros := c.hit(nonce, numTrailingZeros, trace, false)
	if cacheVal == nil && hitZeros > 0 {
		// numTrailingZeros lower than hit
		c.remove(nonce, numTrailingZeros, trace)
	} else if cacheVal != nil && hitZeros == numTrailingZeros {
		// numTrailingZeros equal to hit
		if bytes.Compare(append([]uint8{cacheByte}, cacheVal...), append([]uint8{workerByte}, secret...)) == -1 {
			log.Println("domination found")
			c.remove(nonce, numTrailingZeros, trace)
		} else {
			return false
		}
	} else if cacheVal != nil && hitZeros > 0 {
		return false
	}
	check := c.addSafe(nonce, numTrailingZeros, workerByte, secret, trace)
	return check
}

//func (c *CacheBox) add(nonce []uint8, numTrailingZeros uint, workerByte uint8, secret []uint8, trace *tracing.Trace) bool {
//	cacheKey := c.CacheEncoder(nonce)
//	log.Println("mu locked")
//	println(nonce)
//	println(numTrailingZeros)
//	println(workerByte)
//	c.mu.Lock()
//	// cacheVal, hitZeros := c.hit(nonce, numTrailingZeros, trace, true)
//	cacheByte, cacheVal, hitZeros := c.hit(nonce, numTrailingZeros, trace, false)
//	if cacheVal == nil && hitZeros > 0 {
//		// numTrailingZeros lower than hit
//		c.remove(nonce, numTrailingZeros, trace)
//	} else if cacheVal != nil && hitZeros == numTrailingZeros {
//		// numTrailingZeros equal to hit
//		if bytes.Compare(append([]uint8{cacheByte}, cacheVal...), append([]uint8{workerByte}, secret...)) == -1 {
//			log.Println("domination found")
//			c.remove(nonce, numTrailingZeros, trace)
//		} else {
//			return false
//		}
//	} else if cacheVal != nil && hitZeros > 0 {
//		return false
//	}
//	log.Println("muRemoveAdd locked")
//	c.muRemoveAdd.Lock()
//	//byteSecret := append([]uint8{workerByte}, secret...)
//	addValue := CacheValue{
//		secret: secret,
//		numTrailingZeros: numTrailingZeros,
//		workerByte: workerByte,
//	}
//	c.cacheBox[cacheKey] = addValue
//	trace.RecordAction(CacheAdd{
//		Nonce: nonce,
//		NumTrailingZeros: numTrailingZeros,
//		Secret: generateByteSecret(addValue.workerByte, addValue.secret),
//	})
//	c.mu.Unlock()
//	log.Println("mu unlocked")
//	c.muRemoveAdd.Unlock()
//	log.Println("muRemoveAdd unlocked")
//	return true
//}

func (c *CacheBox) getWorkerByte(nonce []uint8) (uint8, bool) {
	cacheKey := c.CacheEncoder(nonce)
	if val, key := c.cacheBox[cacheKey]; key {
		return val.workerByte, true
	}
	return 0, false
}

// []uint8 == nil && uint == 0 -> nothing in the cache
// []uint8 == nil && uint == numTrailingZeros -> have one but numTraillingZeros is lower
// []uint8 == not nil && uint == val.numTrailingZeros -> cache hit
func (c *CacheBox) hit(nonce []uint8, numTrailingZeros uint, trace *tracing.Trace, silence bool) (uint8, []uint8, uint) {
	cacheKey := c.CacheEncoder(nonce)
	if val, key := c.cacheBox[cacheKey]; key {
		if val.numTrailingZeros >= numTrailingZeros {
			if !silence {
				trace.RecordAction(CacheHit{
					Nonce: nonce,
					NumTrailingZeros: numTrailingZeros,
					Secret: generateByteSecret(val.workerByte, val.secret),
				})
			}
			return val.workerByte, val.secret, val.numTrailingZeros
		} else {
			if !silence {
				trace.RecordAction(CacheMiss{
					Nonce: nonce,
					NumTrailingZeros: numTrailingZeros,
				})
			}
			return val.workerByte, nil, numTrailingZeros
		}
	}
	if !silence {
		trace.RecordAction(CacheMiss{
			Nonce: nonce,
			NumTrailingZeros: numTrailingZeros,
		})
	}

	return 0, nil, 0
}

func (c *CacheBox) CacheEncoder(nonce []uint8) string {
	var caches bytes.Buffer
	enc := gob.NewEncoder(&caches)
	err := enc.Encode(nonce)
	if err != nil {
		log.Fatal("Caching failed!")
	}
	return caches.String()
}