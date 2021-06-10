package distkvs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type StorageLog struct {
	//Time		   	string
	FrontEndOpsNum uint64
	Key            string
	Value          string
}

type StorageInfo StorageFrontEndJoinReqArgs

type StorageFrontEndJoinReqArgs struct {
	LatestFrontEndOpsNum uint64
	StorageAddress       string
	StorageID            string
}

type StorageFrontEndJoinReply struct {
	Connected bool
}

type FrontEndStorageArgs struct {
	OpID               uint32
	Key                string
	Value              string
	FrontEndOpsNum     uint64
	FrontEndTraceToken tracing.TracingToken
	StorageId          string
}

type FrontEndStorageReply struct {
	OpID              uint32
	Key               string
	Value             *string
	StorageFailed     bool
	StorageTraceToken tracing.TracingToken
}

type StorageResultChannel struct {
	OpID              uint32
	Key               string
	Value             string
	StorageTraceToken tracing.TracingToken
	MethodType        string
	StorageFailed     bool
}

type StorageFrontEndArgs struct {
	OpID              uint32
	Key               string
	Value             string
	StorageFailed     bool
	StorageTraceToken tracing.TracingToken
}

type StorageFrontEndReply struct {
	Result bool
}

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	StorageID string
	State     map[string]string
	//The storage records this action just after successful loading of Key-Value state from disk
	//(or initializing this state, if it does not exist).
	//It should report all key-value pairs that were loaded as state,
	//which is a Go map.
}

type StorageJoining struct {
	StorageID string
}

//StorageJoined{StorageID, state}
type StorageJoined struct {
	StorageID string
	State     map[string]string
	//map[key]value  , all k-v pairs of current storage
	//The node should report all key-value pairs that were loaded
	//from disk and received as part of the catch up process as state,
	//which is a Go map.
}

type StoragePut struct {
	StorageID string
	Key   string
	Value string
}

type StorageSaveData struct {
	StorageID string
	Key   string
	Value string
}

type StorageGet struct {
	StorageID string
	Key string
}

type StorageGetResult struct {
	StorageID string
	Key   string
	Value *string
}

type Storage struct {
	// state may go here
	FrontEnd  *rpc.Client
	FileName  string
	StorageId string
}

type StorageRPCHandler struct {
	localTrace	   *tracing.Trace
	tracer         *tracing.Tracer
	frontEnd       *rpc.Client
	cacheBox       map[string]string
	diskPath       string
	putLock        map[string]*sync.Mutex
	catchUpPutLock sync.Mutex
}

type FrontEndStoragePrimaryCatchUpArgs struct {
	FrontEndOpsNum uint64
}

type FrontEndStoragePrimaryCatchUpReply struct {
	CatchUpState []string
}

type FrontEndStorageBackupCatchUpArgs FrontEndStoragePrimaryCatchUpReply
type FrontEndStorageBackupCatchUpReply FrontEndStoragePrimaryCatchUpArgs

// RPC

// CatchUpBackup Backup node RPC, frontend send catchup state to this node
func (s *StorageRPCHandler) CatchUpBackup(args FrontEndStorageBackupCatchUpArgs, reply *FrontEndStorageBackupCatchUpReply) error {
	f, err := os.OpenFile(s.diskPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var storageEntry StorageLog
	for i := 0; i < len(args.CatchUpState); i++ {
		_, err2 := f.WriteString(args.CatchUpState[i] + "\n")
		if err2 != nil {
			log.Fatal(err2)
		}

		errJson := json.Unmarshal([]byte(args.CatchUpState[i]), &storageEntry)
		if errJson != nil {
			log.Fatal(errJson)
		}
		s.cacheBox[storageEntry.Key] = storageEntry.Value
	}

	reply.FrontEndOpsNum = storageEntry.FrontEndOpsNum

	return nil
}

// CatchUpPrimary Primary node RPC, node send catchup state to frontend
func (s *StorageRPCHandler) CatchUpPrimary(args FrontEndStoragePrimaryCatchUpArgs, reply *FrontEndStoragePrimaryCatchUpReply) error {
	s.catchUpPutLock.Lock()
	defer s.catchUpPutLock.Unlock()
	f, e := os.Open(s.diskPath)
	if e != nil {
		log.Fatal("file error")
	}
	var result []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var storageEntry StorageLog
		errJson := json.Unmarshal([]byte(scanner.Text()), &storageEntry)
		if errJson != nil {
			log.Fatal(errJson)
		}
		if args.FrontEndOpsNum < storageEntry.FrontEndOpsNum {
			result = append(result, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	reply.CatchUpState = result

	return nil
}

func (s *StorageRPCHandler) Put(args FrontEndStorageArgs, reply *FrontEndStorageReply) error {
	s.catchUpPutLock.Lock()
	defer s.catchUpPutLock.Unlock()
	_, ok := s.putLock[args.Key]
	if !ok {
		s.putLock[args.Key] = new(sync.Mutex)
	}
	s.putLock[args.Key].Lock()
	defer s.putLock[args.Key].Unlock()

	storageFailed := false
	var storageTrace *tracing.Trace
	if s.tracer != nil {
		storageTrace = s.tracer.ReceiveToken(args.FrontEndTraceToken)

	}

	// Change in memory
	s.cacheBox[args.Key] = args.Value
	if s.tracer != nil {
		storageTrace.RecordAction(StoragePut{
			StorageID: args.StorageId,
			Key:   args.Key,
			Value: args.Value,
		})
	}

	// Change in disk
	err := s.Write(args.Key, args.Value, args.FrontEndOpsNum, storageTrace)
	if err != nil {
		log.Println(err)
		storageFailed = true
	}

	if s.tracer != nil {
		storageTrace.RecordAction(StorageSaveData{
			StorageID: args.StorageId,
			Key:   args.Key,
			Value: args.Value,
		})
	}

	// reply
	reply.OpID = args.OpID
	reply.Key = args.Key
	reply.Value = &args.Value
	reply.StorageFailed = storageFailed
	if s.tracer != nil {
		reply.StorageTraceToken = storageTrace.GenerateToken()

	}

	return nil
}

// RPC
func (s *StorageRPCHandler) Get(args FrontEndStorageArgs, reply *FrontEndStorageReply) error {
	_, ok := s.putLock[args.Key]
	if !ok {
		s.putLock[args.Key] = new(sync.Mutex)
	}
	s.putLock[args.Key].Lock()
	defer s.putLock[args.Key].Unlock()

	// get trace
	var storageTrace *tracing.Trace
	if s.tracer != nil {
		storageTrace = s.tracer.ReceiveToken(args.FrontEndTraceToken)

	}

	storageFailed := false

	// record StorageGet
	if s.tracer != nil {
		storageTrace.RecordAction(StorageGet{
			StorageID: args.StorageId,
			Key: args.Key,
		})
	}

	// get value
	val := s.Read(args.Key)
	var p *string
	if val == "" {
		p = nil
	} else {
		p = &val
	}
	if s.tracer != nil {
		storageTrace.RecordAction(StorageGetResult{
			StorageID: args.StorageId,
			Key:   args.Key,
			Value: p,
		})
	}

	// reply
	reply.OpID = args.OpID
	reply.Key = args.Key
	reply.Value = p
	reply.StorageFailed = storageFailed
	if s.tracer != nil {
		reply.StorageTraceToken = storageTrace.GenerateToken()

	}

	return nil
}

func (s *StorageRPCHandler) Read(key string) string {
	if val, key := s.cacheBox[key]; key {
		return val
	} else {
		return ""
	}
}

func (s *StorageRPCHandler) Write(key string, value string, frontEndOpsNum uint64, storageTrace *tracing.Trace) error {
	s.cacheBox[key] = value
	j, err := json.Marshal(&StorageLog{
		//Time: time.Now().String(),
		FrontEndOpsNum: frontEndOpsNum,
		Key:            key,
		Value:          value,
	})
	if err != nil {
		return err
	}

	f, err := os.OpenFile(s.diskPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	_, err2 := f.WriteString(string(j) + "\n")

	if err2 != nil {
		return err2
	}

	return nil
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	// New storage
	frontendClient, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		log.Fatal("failed to dail FrontEnd:", err)
	}
	s.FrontEnd = frontendClient
	s.FileName = "DiskStorage.json"
	s.StorageId = storageId
	// Fire up RPCs
	server := rpc.NewServer()
	var storageTrace *tracing.Trace
	if strace != nil {
		storageTrace = strace.CreateTrace()
	}

	// read disk
	filePath := diskPath + s.FileName
	cacheFromDisk := make(map[string]string)
	if !fileExists(filePath) {
		_, err := os.Create(filePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	f, e := os.Open(filePath)
	if e != nil {
		log.Fatal("file error")
	}

	var latestFrontEndOpsNum uint64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {

		var storageEntry StorageLog
		errJson := json.Unmarshal([]byte(scanner.Text()), &storageEntry)
		if errJson != nil {
			log.Fatal(errJson)
		}
		cacheFromDisk[storageEntry.Key] = storageEntry.Value
		latestFrontEndOpsNum = storageEntry.FrontEndOpsNum
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if strace != nil {
		storageTrace.RecordAction(StorageLoadSuccess{
			StorageID: storageId,
			State:     cacheFromDisk,
		})
	}

	if strace != nil {
		storageTrace.RecordAction(StorageJoining{
			StorageID: storageId,
		})
	}

	// Start listen
	err = server.Register(&StorageRPCHandler{
		localTrace: storageTrace,
		tracer:   strace,
		frontEnd: s.FrontEnd,
		cacheBox: cacheFromDisk,
		diskPath: diskPath + s.FileName,
		putLock:  make(map[string]*sync.Mutex),
	})

	// publish Storage RPCs
	if err != nil {
		return fmt.Errorf("format of Storage RPCs aren't correct: %s", err)
	}

	listener, e := net.Listen("tcp", storageAddr)
	if e != nil {
		return fmt.Errorf("storage node listen error: %s", e)
	}

	log.Printf("Serving storage node RPCs on port %s", storageAddr)

	go server.Accept(listener)

	// Ping FrontEnd
	joinReqArgs := StorageFrontEndJoinReqArgs{
		LatestFrontEndOpsNum: latestFrontEndOpsNum,
		StorageID:            storageId,
		StorageAddress:       storageAddr,
	}
	var joinReply StorageFrontEndJoinReply
	retry := 0
	for retry < 20 {
		log.Println("Trying to connect FrontEnd...")
		err := s.FrontEnd.Call("FrontEndHandler.Join", joinReqArgs, &joinReply)
		if err != nil {
			log.Fatal(err)
		}
		if joinReply.Connected {
			break
		}
		retry++
		time.Sleep(2 * time.Second)
	}

	if retry >= 20 {
		log.Fatal("Failed to connect FrontEnd!")
	} else {
		log.Println("Connected to the FrontEnd!")
	}

	if strace != nil {
		storageTrace.RecordAction(StorageJoined{
			StorageID: storageId,
			State:     cacheFromDisk,
		})
	}

	stopChan := make(chan struct{})
	<- stopChan

	return nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
