// Package powlib provides an API which is a wrapper around RPC calls to the
// coordinator.
package powlib

import (
	"encoding/hex"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net/rpc"
	"sync"
)

type PowlibMiningBegin struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type PowlibMiningComplete struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

// MineResult contains the result of a mining request.
type MineResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte 		 uint8
	Secret           []uint8
	CoordTraceToken  tracing.TracingToken
}

// Contain trace when call coordinator
type CoordMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	ClientTraceToken tracing.TracingToken
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan MineResult

// CloseChannel is used for notifying powlib goroutines of powlib Close event
type CloseChannel chan struct{}

// POW struct represents an instance of the powlib.
type POW struct {
	coordinator 	*rpc.Client
	notifyCh    	NotifyChannel
	closeCh     	CloseChannel
	closeWg     	*sync.WaitGroup
	//clientTrace 	*tracing.Trace
	mineTasks		map[string]*sync.Mutex
	muMap			sync.Mutex
}

func NewPOW() *POW {
	return &POW{
		coordinator: nil,
		notifyCh:    nil,
		closeWg:     nil,
		//clientTrace: nil,
		mineTasks:   make(map[string]*sync.Mutex),
	}
}

// Initialize Initializes the instance of POW to use for connecting to the coordinator,
// and the coordinators IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by powlib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (p *POW) Initialize(coordAddr string, chCapacity uint) (NotifyChannel, error) {
	// connect to Coordinator
	log.Printf("dailing coordinator at %s", coordAddr)
	coordinator, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing coordinator: %s", err)
	}
	p.coordinator = coordinator

	// create notify channel with given capacity
	p.notifyCh = make(NotifyChannel, chCapacity)
	p.closeCh = make(CloseChannel, chCapacity)

	var wg sync.WaitGroup
	p.closeWg = &wg

	return p.notifyCh, nil
}

// Mine is a non-blocking request from the client to the system solve a proof
// of work puzzle. The arguments have identical meaning as in A2. In case
// there is an underlying issue (for example, the coordinator cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking, and the solution to the proof of work
// puzzle must be delivered asynchronously to the client via the notify-channel
// channel returned in the Initialize call.
func (p *POW) Mine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeros uint) error {
	clientTrace := tracer.CreateTrace()
	clientTrace.RecordAction(PowlibMiningBegin{
		Nonce:            nonce,
		NumTrailingZeros: numTrailingZeros,
	})
	p.closeWg.Add(1)
	go p.callMine(tracer, clientTrace, nonce, numTrailingZeros)
	return nil
}

// Close Stops the POW instance from communicating with the coordinator and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (p *POW) Close() error {

	// notify all callMine goroutines of close
	p.closeCh <- struct{}{}
	p.closeWg.Wait()
	log.Println("All callMine routine existed")

	// close coordinator connection
	err := p.coordinator.Close()
	if err != nil {
		return err
	}
	p.coordinator = nil
	log.Println("Closed Coordinator connection")

	return nil
}

func (p *POW) callMine(tracer *tracing.Tracer, trace *tracing.Trace, nonce []uint8, numTrailingZeros uint) {
	defer func() {
		log.Printf("callMine done")
		p.closeWg.Done()
	}()

	args := CoordMineArgs{
		Nonce:            nonce,
		NumTrailingZeros: numTrailingZeros,
	}

	trace.RecordAction(PowlibMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
	})

	result := MineResult{}

	p.muMap.Lock()
	_, ok := p.mineTasks[generateMineTaskKey(nonce)]
	if !ok {
		p.mineTasks[generateMineTaskKey(nonce)] = new(sync.Mutex)
	}
	val := p.mineTasks[generateMineTaskKey(nonce)]
	p.muMap.Unlock()
	val.Lock()
	// Create trace token
	args.ClientTraceToken = trace.GenerateToken()
	call := p.coordinator.Go("CoordRPCHandler.Mine", args, &result, nil)
	defer val.Unlock()

	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				trace = tracer.ReceiveToken(result.CoordTraceToken)
				trace.RecordAction(PowlibSuccess{
					Nonce:            result.Nonce,
					NumTrailingZeros: result.NumTrailingZeros,
					Secret:           generateByteSecret(result.WorkerByte, result.Secret),
				})
				trace.RecordAction(PowlibMiningComplete{
					Nonce:            result.Nonce,
					NumTrailingZeros: result.NumTrailingZeros,
					Secret:           generateByteSecret(result.WorkerByte, result.Secret),
				})
				p.notifyCh <- result
			}
			return
		case <-p.closeCh:
			log.Printf("cancel callMine")
			p.closeCh <- struct{}{}
			return
		}
	}

}

func generateMineTaskKey(nonce []uint8) string {
	return fmt.Sprintf("%s", hex.EncodeToString(nonce))
}

func generateByteSecret(bytePrefix uint8, secret []uint8) []uint8 {
	return secret
}
