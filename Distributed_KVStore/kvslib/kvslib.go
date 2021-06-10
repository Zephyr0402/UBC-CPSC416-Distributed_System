// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontEnd.
package kvslib

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net/rpc"
	"sync"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvLibFrontEndReqArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type KvsLibFrontEndReply struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
	Token tracing.TracingToken
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type KVS struct {
	notifyCh      NotifyChannel
	getResChannel chan *rpc.Call
	putResChannel chan *rpc.Call

	opIdChannel chan uint32

	frontEnd *rpc.Client
	//CurrentOpId uint32
	lock *sync.Mutex
	//localTracer *tracing.Tracer
	localTrace *tracing.Trace
	clientId   string

	// Add more KVS instance state here.
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		lock:     &sync.Mutex{},
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontEnd,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	log.Printf("dailing frontEnd at %s", frontEndAddr)
	frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing frontEnd: %s", err)
	}
	if localTracer != nil {
		localTrace := localTracer.CreateTrace()
		localTrace.RecordAction(KvslibBegin{ClientId: clientId})
		d.localTrace = localTrace
	}

	d.frontEnd = frontEnd
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.getResChannel = make(chan *rpc.Call, chCapacity*10)
	d.putResChannel = make(chan *rpc.Call, chCapacity*10)
	d.opIdChannel = make(chan uint32, 1000)
	//d.localTracer = localTracer

	d.clientId = clientId
	go d.opIdGenerator()
	go d.resReceiver(localTracer)
	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return OpId or error
	opId := <-d.opIdChannel
	var token tracing.TracingToken
	if d.localTrace != nil {
		trace := tracer.CreateTrace()

		trace.RecordAction(KvslibGet{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
		})

		token = trace.GenerateToken()
	}

	d.frontEnd.Go("FrontEndHandler.Get", &KvLibFrontEndReqArgs{
		ClientId: clientId,
		OpId:     opId,
		Key:      key,
		Token:    token,
	}, &KvsLibFrontEndReply{}, d.getResChannel)

	return opId, nil
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return OpId or error
	opId := <-d.opIdChannel
	var token tracing.TracingToken
	if d.localTrace != nil {
		trace := tracer.CreateTrace()
		trace.RecordAction(KvslibPut{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
			Value:    value,
		})
		token = trace.GenerateToken()

	}

	d.frontEnd.Go("FrontEndHandler.Put", &KvLibFrontEndReqArgs{
		ClientId: clientId,
		OpId:     opId,
		Key:      key,
		Value:    value,
		Token:    token,
	}, &KvsLibFrontEndReply{}, d.putResChannel)

	return opId, nil
}

// Close Stops the KVS instance from communicating with the frontEnd and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {

	//https://piazza.com/class/kjacvyindn53om?cid=571
	err := d.frontEnd.Close()
	if d.localTrace != nil {
		d.localTrace.RecordAction(KvslibComplete{d.clientId})

	}

	return err
}

func (d *KVS) opIdGenerator() { //generate opId, send it to d.opIdChannel
	id := uint32(0)
	for {
		d.opIdChannel <- id
		id++
	}
}

func (d *KVS) resReceiver(tracer *tracing.Tracer) {
	// d.getResChannel and d.putResChannel get response from frontend first,
	//then resReceiver send this response to d.notifyCh then to the client
	for {
		select {
		case res := <-d.getResChannel:
			if res.Error != nil {
				return
			}
			getRes := res.Reply.(*KvsLibFrontEndReply)
			if tracer != nil {
				trace := tracer.ReceiveToken(getRes.Token)
				trace.RecordAction(KvslibGetResult{
					OpId:  getRes.OpId,
					Key:   getRes.Key,
					Value: getRes.Value,
					Err:   getRes.Err,
				})
			}

			d.notifyCh <- ResultStruct{
				OpId:        getRes.OpId,
				StorageFail: getRes.Err,
				Result:      getRes.Value,
			}

		case res := <-d.putResChannel:
			if res.Error != nil {
				return
			}
			putRes := res.Reply.(*KvsLibFrontEndReply)
			if tracer != nil {
				trace := tracer.ReceiveToken(putRes.Token)
				trace.RecordAction(KvslibPutResult{
					OpId: putRes.OpId,
					Err:  putRes.Err,
				})
			}

			d.notifyCh <- ResultStruct{
				OpId:        putRes.OpId,
				StorageFail: putRes.Err,
				Result:      putRes.Value,
			}

		}

	}
}
