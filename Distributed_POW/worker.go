package distpow

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

/****** Tracer structs ******/
type WorkerConfig struct {
	WorkerID         string
	ListenAddr       string
	CoordAddr        string
	TracerServerAddr string
	TracerSecret     []byte
}

type WorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type WorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type WorkerResultWithTrace struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	WorkerTrace      *tracing.Trace
}

type WorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

/****** RPC structs ******/
type WorkerMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	WorkerBits       uint
	CoordTraceToken  tracing.TracingToken
}

type WorkerFoundArgs struct {
	Nonce            []uint8
	Secret           []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	FoundWorkerByte  uint8
	CoordTraceToken  tracing.TracingToken
}

type WorkerCoordReply struct {
	CoordTraceToken tracing.TracingToken
}

type CancelChan chan struct{}

type Worker struct {
	config        WorkerConfig
	Tracer        *tracing.Tracer
	Coordinator   *rpc.Client
	mineTasks     map[string]CancelChan
	ResultChannel chan WorkerResultWithTrace
}

type WorkerMineTasks struct {
	mu    sync.Mutex
	tasks map[string]CancelChan
}

type WorkerRPCHandler struct {
	tracer      *tracing.Tracer
	coordinator *rpc.Client
	mineTasks   WorkerMineTasks
	resultChan  chan WorkerResultWithTrace
	cacheBox    *CacheBox
	muGetTask   sync.Mutex
}

func NewWorker(config WorkerConfig) *Worker {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.WorkerID,
		Secret:         config.TracerSecret,
	})

	coordClient, err := rpc.Dial("tcp", config.CoordAddr)
	if err != nil {
		log.Fatal("failed to dail Coordinator:", err)
	}

	return &Worker{
		config:        config,
		Tracer:        tracer,
		Coordinator:   coordClient,
		mineTasks:     make(map[string]CancelChan),
		ResultChannel: make(chan WorkerResultWithTrace),
	}
}

func (w *Worker) InitializeWorkerRPCs() error {
	server := rpc.NewServer()
	err := server.Register(&WorkerRPCHandler{
		tracer:      w.Tracer,
		coordinator: w.Coordinator,
		mineTasks: WorkerMineTasks{
			tasks: make(map[string]CancelChan),
		},
		resultChan: w.ResultChannel,
		cacheBox:   NewCacheBox(),
	})

	// publish Worker RPCs
	if err != nil {
		return fmt.Errorf("format of Worker RPCs aren't correct: %s", err)
	}

	listener, e := net.Listen("tcp", w.config.ListenAddr)
	if e != nil {
		return fmt.Errorf("%s listen error: %s", w.config.WorkerID, e)
	}

	log.Printf("Serving %s RPCs on port %s", w.config.WorkerID, w.config.ListenAddr)
	go server.Accept(listener)

	return nil
}

// Mine is a non-blocking async RPC from the Coordinator
// instructing the worker to solve a specific pow instance.
func (w *WorkerRPCHandler) Mine(args WorkerMineArgs, reply *CoordWorkerReply) error {
	// Receive coordinator trace token
	workerTrace := w.tracer.ReceiveToken(args.CoordTraceToken)

	mineInfo := WorkerMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
	}

	// add new task
	cancelCh := make(chan struct{}, 1)
	w.mineTasks.set(args.Nonce, args.NumTrailingZeros, args.WorkerByte, cancelCh)

	workerTrace.RecordAction(WorkerMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
		WorkerByte:       args.WorkerByte,
	})

	// Check caching hits
	cacheByte, cacheSecret, hitZeros := w.cacheBox.hit(mineInfo.Nonce, mineInfo.NumTrailingZeros, workerTrace, false)
	if cacheSecret != nil && hitZeros > 0 {
		// Cache hit
		workerTrace.RecordAction(WorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           generateByteSecret(cacheByte, cacheSecret),
		})
		w.resultChan <- WorkerResultWithTrace{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           generateByteSecret(cacheByte, cacheSecret),
			WorkerTrace:      workerTrace,
		}
		return nil
	}

	go miner(w, args, cancelCh, workerTrace, args.CoordTraceToken)

	reply.WorkerTraceToken = args.CoordTraceToken

	return nil
}

// Found is a non-blocking async RPC from the Coordinator
// instructing the worker to stop solving a specific pow instance and
// caching result.
func (w *WorkerRPCHandler) Found(args WorkerFoundArgs, reply *CoordWorkerReply) error {
	// Receive tracing token
	workerTrace := w.tracer.ReceiveToken(args.CoordTraceToken)

	cancelChan, ok := w.mineTasks.get(args.Nonce, args.NumTrailingZeros, args.FoundWorkerByte)
	//if !ok {
	//	log.Fatalf("Received more than once cancellation for %s", generateWorkerTaskKey(args.Nonce, args.NumTrailingZeros, args.WorkerByte))
	//}

	if ok {
		cancelChan <- struct{}{}
		// delete the task here, and the worker should terminate + send something back very soon
		w.mineTasks.delete(args.Nonce, args.NumTrailingZeros, args.FoundWorkerByte)
	} else {
		workerTrace.RecordAction(WorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.FoundWorkerByte,
		})
	}

	// Caching result from coordinator
	w.cacheBox.add(args.Nonce, args.NumTrailingZeros, args.WorkerByte, args.Secret, workerTrace)

	// Send back tracing token
	reply.WorkerTraceToken = workerTrace.GenerateToken()

	return nil
}

func nextChunk(chunk []uint8) []uint8 {
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == 0xFF {
			chunk[i] = 0
		} else {
			chunk[i]++
			return chunk
		}
	}
	return append(chunk, 1)
}

func hasNumZeroesSuffix(str []byte, numZeroes uint) bool {
	var trailingZeroesFound uint
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '0' {
			trailingZeroesFound++
		} else {
			break
		}
	}
	return trailingZeroesFound >= numZeroes
}

func miner(w *WorkerRPCHandler, args WorkerMineArgs, killChan <-chan struct{}, workerTrace *tracing.Trace, workerTraceToken tracing.TracingToken) {
	chunk := []uint8{}
	remainderBits := 8 - (args.WorkerBits % 9)

	hashStrBuf, wholeBuffer := new(bytes.Buffer), new(bytes.Buffer)
	if _, err := wholeBuffer.Write(args.Nonce); err != nil {
		panic(err)
	}
	wholeBufferTrunc := wholeBuffer.Len()

	// table out all possible "thread bytes", aka the byte prefix
	// between the nonce and the bytes explored by this worker
	remainderEnd := 1 << remainderBits
	threadBytes := make([]uint8, remainderEnd)
	for i := 0; i < remainderEnd; i++ {
		threadBytes[i] = uint8((int(args.WorkerByte) << remainderBits) | i)
	}

	for {
		for _, threadByte := range threadBytes {
			select {
			case <-killChan:
				workerTrace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				w.resultChan <- WorkerResultWithTrace{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil, // nil secret treated as cancel completion
					WorkerTrace:      workerTrace,
				}
				return
			default:
				// pass
			}
			wholeBuffer.Truncate(wholeBufferTrunc)
			if err := wholeBuffer.WriteByte(threadByte); err != nil {
				panic(err)
			}
			if _, err := wholeBuffer.Write(chunk); err != nil {
				panic(err)
			}
			hash := md5.Sum(wholeBuffer.Bytes())
			hashStrBuf.Reset()
			fmt.Fprintf(hashStrBuf, "%x", hash)
			if hasNumZeroesSuffix(hashStrBuf.Bytes(), args.NumTrailingZeros) {
				result := WorkerResultWithTrace{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
					WorkerTrace:      workerTrace,
				}
				workerTrace.RecordAction(WorkerResult{
					Nonce:            result.Nonce,
					NumTrailingZeros: result.NumTrailingZeros,
					WorkerByte:       result.WorkerByte,
					Secret:           generateByteSecret(result.WorkerByte, result.Secret),
				})

				w.resultChan <- result

				// Caching result
				w.cacheBox.add(args.Nonce, args.NumTrailingZeros, result.WorkerByte, result.Secret, workerTrace)

				// now, wait for the worker the receive a cancellation,
				// which the coordinator should always send no matter what.
				// note: this position takes care of interleavings where cancellation comes after we check killChan but
				//       before we log the result we found, forcing WorkerCancel to be the last action logged, even in that case.
				<-killChan

				// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
				workerTrace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				// ACK the cancellation; the coordinator will be waiting for this.
				w.resultChan <- WorkerResultWithTrace{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil,
					WorkerTrace:      workerTrace,
				}

				return
			}
		}
		chunk = nextChunk(chunk)
	}
}

func (t *WorkerMineTasks) get(nonce []uint8, numTrailingZeros uint, workerByte uint8) (CancelChan, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)]
	return t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)], ok
}

func (t *WorkerMineTasks) set(nonce []uint8, numTrailingZeros uint, workerByte uint8, val CancelChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)] = val
}

func (t *WorkerMineTasks) delete(nonce []uint8, numTrailingZeros uint, workerByte uint8) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateWorkerTaskKey(nonce, numTrailingZeros, workerByte))
}

func generateWorkerTaskKey(nonce []uint8, numTrailingZeros uint, workerByte uint8) string {
	return fmt.Sprintf("%s|%d|%d", hex.EncodeToString(nonce), numTrailingZeros, workerByte)
}

func generateByteSecret(bytePrefix uint8, secret []uint8) []uint8 {
	return secret
}
