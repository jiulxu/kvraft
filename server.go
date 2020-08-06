package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"strconv"
	"bytes"
	"os"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func DPrintfToFile(id int, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fname := strconv.Itoa(id) + ".txt"
		f, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		c := time.Now().String() + "    " +fmt.Sprintf(format, a...)
		f.WriteString(c)
		f.Close()
		log.Printf(format, a...)
	}
	return
}

const (
	GET = "Get"
	PUT = "Put"
	APPEND = "Append"
)

type OpType string

const (
	EXEC = "Exec"
	WRONG_LEADER = "WrongLeader"
	READ_CACHE = "ReadCache"
)

type StateOpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type OpType
	Key string
	Value string
	ClientId int64
	ClientSeq int
}

type CacheEntry struct {
	Seq int
	Reply *GetReply
}

type RetInfo struct {
	retCh chan StateOpType
	reply *GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string
	retMap map[int]*RetInfo
	cacheMap map[int64]*CacheEntry
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintfToFile(kv.me, "server:%d received Get \n", kv.me)
	// If this is a retry of something already commited, retrun cached data.
	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 6)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 6)
	if cache, ok := kv.cacheMap[args.ClientId]; ok {
		if cache.Seq == args.ClientSeq {
			*reply = *cache.Reply
			DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 6)
			kv.mu.Unlock()
			return
		}
	}
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 6)
	kv.mu.Unlock()

	op := Op{}
	op.Type = GET
	op.Key = args.Key
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq

	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 5)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 5)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err  = ErrWrongLeader
		kv.mu.Unlock()
		DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 5)
		return
	}
	DPrintfToFile(kv.me, "server: %d is leader. start handle get index: %d\n", kv.me, index)
	ret := RetInfo{}
	ret.retCh = make(chan StateOpType, 1)
	kv.retMap[index] = &ret
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 5)
	kv.mu.Unlock()

	stateOp := <- ret.retCh
	DPrintfToFile(kv.me, "server:%d Get done waiting for retMap[%d] \n", kv.me, index)

	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 4)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 4)
	defer kv.mu.Unlock()
	if stateOp == EXEC {
		*reply = *kv.retMap[index].reply
	} else if stateOp == READ_CACHE {
		*reply = *kv.cacheMap[args.ClientId].Reply
	} else {
		reply.Err = ErrWrongLeader
	}
	delete(kv.retMap, index)
	DPrintfToFile(kv.me, "server:%d finised Get index: %d\n", kv.me, index)
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 4)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintfToFile(kv.me, "server:%d received %s \n", kv.me, args.Op)
	// If this is a retry of something already commited, retrun cached data.
	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 3)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 3)
	if cache, ok := kv.cacheMap[args.ClientId]; ok {
		if cache.Seq == args.ClientSeq {
			// Only OK reply for PutAppend can be cached.
			reply.Err = OK
			DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 3)
			kv.mu.Unlock()
			return
		}
	}
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 3)
	kv.mu.Unlock()

	op := Op{}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq

	// Hold lock to make sure applyChan won't finish before retMap is updated.
	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 2)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 2)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintfToFile(kv.me, "server:%d PA nonleader return \n", kv.me)
		kv.mu.Unlock()
		return
	}
	DPrintfToFile(kv.me, "server: %d is leader. start handle put index: %d\n", kv.me, index)
	ret := RetInfo{}
	ret.retCh = make(chan StateOpType, 10)
	kv.retMap[index] = &ret
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 2)
	kv.mu.Unlock()

	stateOp := <- ret.retCh
	if stateOp == EXEC || stateOp == READ_CACHE{
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintfToFile(kv.me, "server:%d PutAppend done waiting for retMap[%d] \n", kv.me, index)

	DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 1)
	kv.mu.Lock()
	DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 1)
	defer kv.mu.Unlock()
	delete(kv.retMap, index)
	DPrintfToFile(kv.me, "server:%d finished %s index: %d\n", kv.me, args.Op, index)
	DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 1)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Snapshot(index, term int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.cacheMap)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, term, snapshot)
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.kvMap) != nil || d.Decode(&kv.cacheMap) != nil {
	  DPrintfToFile(kv.me, "%d read snapshot error\n", kv.me)
	}
	for _, v := range kv.retMap {
		v.retCh <- WRONG_LEADER
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.retMap = make(map[int]*RetInfo)
	kv.kvMap = make(map[string]string)
	kv.cacheMap = make(map[int64]*CacheEntry)

	// init from snapshot.
	kv.ReadSnapshot(persister.ReadSnapshot())

	go func () {
		for applyMsg := range kv.applyCh {
			DPrintfToFile(kv.me, "server:%d attempt lock %d \n", kv.me, 0)
			kv.mu.Lock()
			DPrintfToFile(kv.me, "server:%d lock %d \n", kv.me, 0)
			if !applyMsg.CommandValid {
				// this is a snapshot, reload state machine.
				kv.ReadSnapshot(applyMsg.Snapshot)
				DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 0)
				kv.mu.Unlock()
				continue
			}
			index := applyMsg.CommandIndex
			DPrintfToFile(kv.me, "server: %d received %d from applyCh \n", kv.me, index)
			op := applyMsg.Command.(Op)

			// First check if this op is a retry.
			if cache, ok := kv.cacheMap[op.ClientId]; ok {
				// if this is a retry, then early return no matter this is leader or not.
				if cache.Seq == op.ClientSeq {
					if ret, ok := kv.retMap[index]; ok {
						ret.retCh <- READ_CACHE
						DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 0)
					}
					kv.mu.Unlock()
					continue
				}
			}

			cache := CacheEntry{}
			cache.Seq = op.ClientSeq
			if op.Type == PUT {
				kv.kvMap[op.Key] = op.Value
			} else if op.Type == APPEND {
				if val, ok := kv.kvMap[op.Key]; ok {
    				kv.kvMap[op.Key] = val + op.Value
				} else {
					// Act like put.
					kv.kvMap[op.Key] = op.Value
				}
			} else {
				reply := GetReply{}
				if val, ok := kv.kvMap[op.Key]; ok {
					reply.Err = OK
					reply.Value = val
				} else {
					reply.Err = ErrNoKey
				}
				cache.Reply = &reply
				// Write reply to retMap.
				if ret, ok := kv.retMap[index]; ok {
					ret.reply = &reply
				}
			}
			kv.cacheMap[op.ClientId] = &cache

			// maybe take a snapshot if perisister is full. Do we need to take snapshot at other places as well?
			if maxraftstate >=0 && persister.RaftStateSize() >= maxraftstate {
				kv.Snapshot(index, applyMsg.Term)
			}

			// send signal to retMap so handler can return to client.
			if ret, ok := kv.retMap[index]; ok {
				DPrintfToFile(kv.me, "server:%d send signal to retMap %d \n", kv.me, index)
				ret.retCh <- EXEC
			}
			kv.mu.Unlock()
			DPrintfToFile(kv.me, "server:%d unlock %d \n", kv.me, 0)
		}
	}()

	go func () {
		for _ = range kv.rf.StepDownCh() {
			kv.mu.Lock()
			for _, v := range kv.retMap {
				v.retCh <- WRONG_LEADER
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
