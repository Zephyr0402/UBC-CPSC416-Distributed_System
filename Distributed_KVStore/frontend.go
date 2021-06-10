package distkvs

import (
	"example.org/cpsc416/a6/kvslib"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageJoined struct {
	StorageIds []string
}
type FrontEndStorageFailed struct {
	StorageID string
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type PutRequest struct {
	OpId  uint32
	Key   string
	Value string
}

type FrontEnd struct {
}

type ReqArgsAndReply struct {
	operation OPERATION
	args      *kvslib.KvLibFrontEndReqArgs
	reply     *kvslib.KvsLibFrontEndReply
	finished  chan bool
}

type FrontEndHandler struct {
	clientAPIListenAddr          string
	storageAPIListenAddr         string
	storageAddr                  string
	storageTimeout               uint8
	tracer                       *tracing.Tracer
	localTrace                   *tracing.Trace
	muStorageFailed              sync.Mutex
	storageAfterFirstTimeConnect map[string]bool
	joinedStorages               map[string]StorageInfo //map[storageId]true
	frontEndOpsNum               uint64
	opChannel                    chan *ReqArgsAndReply
}

type KvsLibFromFrontEndReply struct {
}

func NewFrontEnd() *FrontEnd {
	return &FrontEnd{}
}

type OPERATION int

const (
	GET OPERATION = iota
	PUT
)

func (f *FrontEndHandler) Join(joinReqArgs StorageFrontEndJoinReqArgs, joinReply *StorageFrontEndJoinReply) error {
	// f.storageAddr = joinReqArgs.StorageAddress
	storageClient, err := rpc.Dial("tcp", joinReqArgs.StorageAddress)
	if err != nil {
		joinReply.Connected = false
		log.Println(err)
	} else {
		if f.tracer != nil {
			if _, ok := f.joinedStorages[joinReqArgs.StorageID]; !ok {
				f.localTrace.RecordAction(FrontEndStorageStarted{joinReqArgs.StorageID})
			}
		}
		if len(f.joinedStorages) > 0 {
			// Dial primary
			var primaryStorageClient *rpc.Client
			var errPrimary error
			for true {
				var key string
				for k, _ := range f.joinedStorages {
					key = k
					break
				}
				primaryStorageClient, errPrimary = rpc.Dial("tcp", f.joinedStorages[key].StorageAddress)
				if errPrimary == nil {
					break
				}
				log.Println("Failed to connect primary storage, retrying...")
				time.Sleep(5 * time.Second)
			}
			var primaryReply FrontEndStoragePrimaryCatchUpReply
			err = primaryStorageClient.Call("StorageRPCHandler.CatchUpPrimary", &FrontEndStoragePrimaryCatchUpArgs{
				FrontEndOpsNum: joinReqArgs.LatestFrontEndOpsNum,
			}, &primaryReply)

			// Dial new backup node
			var backupReply FrontEndStorageBackupCatchUpReply
			err = storageClient.Call("StorageRPCHandler.CatchUpBackup", &FrontEndStorageBackupCatchUpArgs{
				CatchUpState: primaryReply.CatchUpState,
			}, &backupReply)

		}

		f.joinedStorages[joinReqArgs.StorageID] = StorageInfo{
			StorageAddress: joinReqArgs.StorageAddress,
			StorageID:      joinReqArgs.StorageID,
		}

		var storageIDs []string
		for storageId, _ := range f.joinedStorages {
			storageIDs = append(storageIDs, storageId)
		}

		if f.tracer != nil {
			f.localTrace.RecordAction(FrontEndStorageJoined{storageIDs})
		}

		joinReply.Connected = true
		//f.storageFailed[joinReqArgs.StorageID] = false
		//f.storageRPCFailed[joinReqArgs.StorageID] = false
		log.Println("Connected to " + joinReqArgs.StorageID)
	}

	return nil
}

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {

	/*
		@588
		ftrace is a frontend Tracer instance;
		use this to (1) create a new distributed trace instance using Tracer.CreateTrace(),
		and (2) record local frontend actions in the created trace that are not associated with any particular client request (see below).

	*/
	var localTrace *tracing.Trace
	if ftrace != nil {
		localTrace = ftrace.CreateTrace()

	} else {
		localTrace = nil
	}
	handler := &FrontEndHandler{
		clientAPIListenAddr:  clientAPIListenAddr,
		storageAPIListenAddr: storageAPIListenAddr,
		storageTimeout:       storageTimeout,
		tracer:               ftrace,
		localTrace:           localTrace,
		opChannel:            make(chan *ReqArgsAndReply, 1000),
		//putChan:              make(chan StorageFrontEndArgs),
		//muPutRequest:         make(map[string]*sync.Mutex),
		//storageFailed:           make(map[string]bool),
		//storageRPCFailed:        make(map[string]bool),
		storageAfterFirstTimeConnect: make(map[string]bool),
		joinedStorages:               make(map[string]StorageInfo),
		frontEndOpsNum:               0,
	}

	rpc.Register(handler)
	inboundFromClient, _ := net.Listen("tcp", clientAPIListenAddr)

	inboundFromStorage, _ := net.Listen("tcp", storageAPIListenAddr)
	go handler.opHandler()
	go rpc.Accept(inboundFromStorage)
	rpc.Accept(inboundFromClient)
	return nil
}

/*
&KvLibFrontEndReqArgs{
ClientId: clientId,
OpId:     opId,
Key:      key,
Token:    trace.GenerateToken()
*/
//https://piazza.com/class/kjacvyindn53om?cid=574

func (f *FrontEndHandler) Get(args *kvslib.KvLibFrontEndReqArgs, reply *kvslib.KvsLibFrontEndReply) error {
	c := make(chan bool, 1)
	f.opChannel <- &ReqArgsAndReply{GET, args, reply, c}
	<-c
	close(c)
	return nil

}
func (f *FrontEndHandler) get(args *kvslib.KvLibFrontEndReqArgs, reply *kvslib.KvsLibFrontEndReply) error {

	var trace *tracing.Trace
	if f.tracer != nil {
		trace = f.tracer.ReceiveToken(args.Token)
		trace.RecordAction(FrontEndGet{args.Key})

	}

	storageNum := len(f.joinedStorages)
	// Send put request to all storage nodes
	getResChan := make(chan *FrontEndStorageReply, storageNum)
	var storageReply *FrontEndStorageReply
	newFrontEndOpsNum := atomic.AddUint64(&f.frontEndOpsNum, 1)
	for _, storageInfo := range f.joinedStorages {

		go f.tryGetOrPut(GET, args, storageInfo, getResChan, newFrontEndOpsNum, trace)

	}
	var frontEndTrace *tracing.Trace
	for i := 0; i < storageNum; i++ {
		reply := <-getResChan
		if reply != nil {
			storageReply = reply
			if f.tracer != nil {
				frontEndTrace = f.tracer.ReceiveToken(storageReply.StorageTraceToken)

			}
		}

	}
	if f.tracer != nil {
		//frontEndTrace := f.tracer.ReceiveToken(storageReply.StorageTraceToken)
		frontEndTrace.RecordAction(FrontEndGetResult{
			Key:   storageReply.Key,
			Value: storageReply.Value,
			Err:   false,
		})
		reply.Token = frontEndTrace.GenerateToken()
	}

	reply.OpId = storageReply.OpID
	reply.Key = storageReply.Key
	reply.Value = storageReply.Value
	reply.Err = false

	return nil
}

func (f *FrontEndHandler) Put(args *kvslib.KvLibFrontEndReqArgs, reply *kvslib.KvsLibFrontEndReply) error {
	c := make(chan bool, 1)
	f.opChannel <- &ReqArgsAndReply{PUT, args, reply, c}
	<-c
	close(c)
	return nil
}

func (f *FrontEndHandler) put(args *kvslib.KvLibFrontEndReqArgs, reply *kvslib.KvsLibFrontEndReply) error {

	var trace *tracing.Trace
	if f.tracer != nil {
		trace = f.tracer.ReceiveToken(args.Token)
		trace.RecordAction(FrontEndPut{args.Key, args.Value})
	}

	storageNum := len(f.joinedStorages)
	// Send put request to all storage nodes
	putResChan := make(chan *FrontEndStorageReply, storageNum)
	var storageReply *FrontEndStorageReply
	newFrontEndOpsNum := atomic.AddUint64(&f.frontEndOpsNum, 1)
	for _, storageInfo := range f.joinedStorages {

		go f.tryGetOrPut(PUT, args, storageInfo, putResChan, newFrontEndOpsNum, trace)

	}
	var frontEndTrace *tracing.Trace
	for i := 0; i < storageNum; i++ {
		reply := <-putResChan
		if reply != nil {
			storageReply = reply
			if f.tracer != nil {
				frontEndTrace = f.tracer.ReceiveToken(storageReply.StorageTraceToken)

			}
		}

	}
	if f.tracer != nil {
		frontEndTrace.RecordAction(FrontEndPutResult{false})
		reply.Token = frontEndTrace.GenerateToken()
	}
	reply.OpId = storageReply.OpID
	reply.Key = storageReply.Key
	reply.Value = storageReply.Value
	reply.Err = storageReply.StorageFailed

	return nil

}

func (f *FrontEndHandler) tryGetOrPut(operation OPERATION, args *kvslib.KvLibFrontEndReqArgs, info StorageInfo, resChan chan *FrontEndStorageReply, frontEndOpsNum uint64, trace *tracing.Trace) {

	storageRPCHandlerStub, err := rpc.Dial("tcp", info.StorageAddress)
	if err != nil {
		time.Sleep(time.Duration(f.storageTimeout) * time.Second)
		storageRPCHandlerStub, err = rpc.Dial("tcp", info.StorageAddress)
		if err != nil {
			trace.RecordAction(FrontEndStorageFailed{info.StorageID})
			resChan <- nil
			delete(f.joinedStorages, info.StorageID)

			var storageIDs []string
			for storageId, _ := range f.joinedStorages {
				storageIDs = append(storageIDs, storageId)
			}

			trace.RecordAction(FrontEndStorageJoined{storageIDs})
			return
		}
	}

	var storageReply FrontEndStorageReply
	var token tracing.TracingToken
	if f.tracer != nil {
		token = trace.GenerateToken()
	}
	var serviceMethod string
	if operation == GET {
		serviceMethod = "StorageRPCHandler.Get"
	} else if operation == PUT {
		serviceMethod = "StorageRPCHandler.Put"

	}

	err = storageRPCHandlerStub.Call(serviceMethod, &FrontEndStorageArgs{
		OpID:               args.OpId,
		Key:                args.Key,
		Value:              args.Value,
		FrontEndOpsNum:     frontEndOpsNum,
		FrontEndTraceToken: token,
	}, &storageReply)

	if err != nil {
		f.tryGetOrPut(operation, args, info, resChan, frontEndOpsNum, trace)
	}

	resChan <- &storageReply

}

func (f *FrontEndHandler) opHandler() {
	for req := range f.opChannel {
		switch req.operation {
		case GET:
			f.get(req.args, req.reply)
			req.finished <- true
		case PUT:
			f.put(req.args, req.reply)
			req.finished <- true

		}

	}

}
