package distpow

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type WorkerAddr string

type WorkerClient struct {
	addr       WorkerAddr
	client     *rpc.Client
	workerByte uint8
}

type CoordinatorConfig struct {
	ClientAPIListenAddr string
	WorkerAPIListenAddr string
	Workers             []WorkerAddr
	TracerServerAddr    string
	TracerSecret        []byte
}

type CoordinatorMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordinatorWorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorWorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type CoordinatorWorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type Coordinator struct {
	config  CoordinatorConfig
	tracer  *tracing.Tracer
	workers []*WorkerClient
}

/****** RPC structs ******/
type CoordMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	ClientTraceToken tracing.TracingToken
}

type CoordMineResponse struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	CoordTraceToken  tracing.TracingToken
}

type CoordResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	WorkerTraceToken tracing.TracingToken
}

type CoordWorkerReply struct {
	WorkerTraceToken tracing.TracingToken
}

type CoordClientReply struct {
	ClientTraceToken tracing.TracingToken
}

type ResultChan chan CoordResultArgs

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	mineTasks  CoordinatorMineTasks
	cacheBox   *CacheBox
	mu         sync.Mutex
}

type CoordinatorMineTasks struct {
	mu    sync.Mutex
	tasks map[string]ResultChan
}

func NewCoordinator(config CoordinatorConfig) *Coordinator {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "coordinator",
		Secret:         config.TracerSecret,
	})

	workerClients := make([]*WorkerClient, len(config.Workers))
	for i, addr := range config.Workers {
		workerClients[i] = &WorkerClient{
			addr:       addr,
			client:     nil,
			workerByte: uint8(i),
		}
	}

	return &Coordinator{
		config:  config,
		tracer:  tracer,
		workers: workerClients,
	}
}

// Mine is a blocking RPC from powlib instructing the Coordinator to solve a specific pow instance
func (c *CoordRPCHandler) Mine(args CoordMineArgs, reply *CoordMineResponse) error {
	// Receive trace token
	coordTrace := c.tracer.ReceiveToken(args.ClientTraceToken)

	mineInfo := CoordinatorMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
	}

	coordTrace.RecordAction(CoordinatorMine{
		NumTrailingZeros: args.NumTrailingZeros,
		Nonce:            args.Nonce,
	})

	// Check caching hits
	cacheByte, cacheSecret, hitZeros := c.cacheBox.hit(mineInfo.Nonce, mineInfo.NumTrailingZeros, coordTrace, false)
	if cacheSecret != nil && hitZeros > 0 {
		// Cache hit
		workerByte, _ := c.cacheBox.getWorkerByte(mineInfo.Nonce)
		coordTrace.RecordAction(CoordinatorWorkerResult{
			Nonce:            mineInfo.Nonce,
			NumTrailingZeros: mineInfo.NumTrailingZeros,
			WorkerByte:       workerByte,
			Secret:           generateByteSecret(cacheByte, cacheSecret),
		})
		coordTrace.RecordAction(CoordinatorSuccess{
			Nonce:            mineInfo.Nonce,
			NumTrailingZeros: mineInfo.NumTrailingZeros,
			Secret:           generateByteSecret(cacheByte, cacheSecret),
		})
		reply.NumTrailingZeros = mineInfo.NumTrailingZeros
		reply.Nonce = mineInfo.Nonce
		reply.Secret = cacheSecret
		reply.CoordTraceToken = coordTrace.GenerateToken()
		reply.WorkerByte = cacheByte
		return nil
	}

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	resultChan := make(chan CoordResultArgs, workerCount)
	c.mineTasks.set(args.Nonce, args.NumTrailingZeros, resultChan)

	for _, w := range c.workers {

		args := WorkerMineArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			WorkerBits:       c.workerBits,
		}

		coordTrace.RecordAction(CoordinatorWorkerMine{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})

		args.CoordTraceToken = coordTrace.GenerateToken()

		reply := CoordWorkerReply{
			WorkerTraceToken: make(tracing.TracingToken, 1),
		}
		err := w.client.Call("WorkerRPCHandler.Mine", args, &reply)
		// c.tracer.ReceiveToken(reply.WorkerTraceToken)

		if err != nil {
			return err
		}
	}

	// wait for at least one result
	result := <-resultChan

	// sanity check
	if result.Secret == nil {
		log.Fatalf("First worker result appears to be cancellation ACK, from workerByte = %d", result.WorkerByte)
	}

	//// Caching result
	//c.cacheBox.add(args.Nonce, args.NumTrailingZeros, result.WorkerByte, result.Secret, coordTrace)
	//
	//// after receiving one result, cancel all workers unconditionally.
	//// the cancellation takes place of an ACK for any workers sending results.
	//for _, w := range c.workers {
	//
	//	coordTrace.RecordAction(CoordinatorWorkerCancel{
	//		Nonce:            args.Nonce,
	//		NumTrailingZeros: args.NumTrailingZeros,
	//		WorkerByte:       w.workerByte,
	//	})
	//
	//	args := WorkerFoundArgs{
	//		Nonce:            args.Nonce,
	//		NumTrailingZeros: args.NumTrailingZeros,
	//		Secret: 		  result.Secret,
	//		WorkerByte:       result.WorkerByte,
	//		FoundWorkerByte:  w.workerByte,
	//		CoordTraceToken:  coordTrace.GenerateToken(),
	//	}
	//
	//	reply := CoordWorkerReply{
	//		WorkerTraceToken: make(tracing.TracingToken, 1),
	//	}
	//	err := w.client.Call("WorkerRPCHandler.Found", args, &reply)
	//	c.tracer.ReceiveToken(reply.WorkerTraceToken)
	//
	//	if err != nil {
	//		return err
	//	}
	//
	//}

	log.Printf("Waiting for %d acks from workers, then we are done", workerCount)

	// wait for all all workers to send back cancel ACK, ignoring results (receiving them is logged, but they have no further use here)
	// we asked all workers to cancel, so we should get exactly workerCount ACKs.
	workerAcksReceived := 0
	for workerAcksReceived < workerCount {
		ack := <-resultChan
		if ack.Secret == nil {
			log.Printf("Counting toward acks: %v", ack)
			workerAcksReceived += 1
		} else {
			log.Printf("Dropping extra result: %v", ack)
		}
	}

	// delete completed mine task from map
	c.mineTasks.delete(args.Nonce, args.NumTrailingZeros)

	coordTrace.RecordAction(CoordinatorSuccess{
		Nonce:            result.Nonce,
		NumTrailingZeros: result.NumTrailingZeros,
		Secret:           generateByteSecret(result.WorkerByte, result.Secret),
	})

	reply.NumTrailingZeros = result.NumTrailingZeros
	reply.Nonce = result.Nonce
	reply.Secret = result.Secret
	reply.CoordTraceToken = coordTrace.GenerateToken()
	reply.WorkerByte = result.WorkerByte

	return nil
}

// Result is a non-blocking RPC from the worker that sends the solution to some previous pow instance assignment
// back to the Coordinator
func (c *CoordRPCHandler) Result(args CoordResultArgs, reply *WorkerCoordReply) error {
	// Receive tracing token
	coordTrace := c.tracer.ReceiveToken(args.WorkerTraceToken)

	if args.Secret != nil {
		coordTrace.RecordAction(CoordinatorWorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           generateByteSecret(args.WorkerByte, args.Secret),
		})
		c.mu.Lock()
		c.cacheBox.add(args.Nonce, args.NumTrailingZeros, args.WorkerByte, args.Secret, coordTrace)
		c.mu.Unlock()
	} else {
		log.Printf("Received worker cancel ack: %v", args)
	}

	c.mineTasks.get(args.Nonce, args.NumTrailingZeros) <- args

	if args.Secret != nil {
		// after receiving one result, cancel all workers unconditionally.
		// the cancellation takes place of an ACK for any workers sending results.
		for _, w := range c.workers {

			coordTrace.RecordAction(CoordinatorWorkerCancel{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				WorkerByte:       w.workerByte,
			})

			args := WorkerFoundArgs{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				Secret:           args.Secret,
				WorkerByte:       args.WorkerByte,
				FoundWorkerByte:  w.workerByte,
				CoordTraceToken:  coordTrace.GenerateToken(),
			}

			reply := CoordWorkerReply{
				WorkerTraceToken: make(tracing.TracingToken, 1),
			}
			err := w.client.Call("WorkerRPCHandler.Found", args, &reply)
			c.tracer.ReceiveToken(reply.WorkerTraceToken)

			if err != nil {
				return err
			}

		}
	}

	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		mineTasks: CoordinatorMineTasks{
			tasks: make(map[string]ResultChan),
		},
		cacheBox: NewCacheBox(),
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of Coordinator RPCs aren't correct: %s", err)
	}

	workerListener, e := net.Listen("tcp", c.config.WorkerAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.WorkerAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", c.config.ClientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.ClientAPIListenAddr, e)
	}

	go server.Accept(workerListener)
	server.Accept(clientListener)

	return nil
}

func initializeWorkers(workers []*WorkerClient) error {
	for _, w := range workers {
		if w.client == nil {
			client, err := rpc.Dial("tcp", string(w.addr))
			if err != nil {
				log.Printf("Waiting for worker %d", w.workerByte)
				return fmt.Errorf("failed to dial worker: %s", err)
			}
			w.client = client
		}
	}
	return nil
}

func (t *CoordinatorMineTasks) get(nonce []uint8, numTrailingZeros uint) ResultChan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)]
}

func (t *CoordinatorMineTasks) set(nonce []uint8, numTrailingZeros uint, val ResultChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)] = val
	log.Printf("New task added: %v\n", t.tasks)
}

func (t *CoordinatorMineTasks) delete(nonce []uint8, numTrailingZeros uint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateCoordTaskKey(nonce, numTrailingZeros))
	log.Printf("Task deleted: %v\n", t.tasks)
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}
