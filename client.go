package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	clientSeq int
	serverId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// We use a random number as client identifier here, currently assume no colision would happen, in reality, client ip address will be a good candidate for this.
	ck.clientId = nrand()
	ck.clientSeq = 0
	ck.serverId = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//DPrintf("client:%d sent Get for key:%s \n", ck.clientId, key)
	// You will have to modify this function.
	ck.clientSeq++
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.ClientSeq = ck.clientSeq
	
	for {
		if ck.serverId == -1 {
			ck.serverId = int(nrand()) % len(ck.servers)
		}
		reply := GetReply{}
		DPrintf("client:%d sends Get to server: %d, %s \n", ck.clientId, ck.serverId, time.Now().String())
		rpcSuccess := ck.servers[ck.serverId].Call("KVServer.Get", &args, &reply)
		DPrintf("client:%d sends Get to server done: %d, %s \n", ck.clientId, ck.serverId, time.Now().String())
		if !rpcSuccess || reply.Err == ErrWrongLeader{
			ck.serverId = -1
			continue
		}

		if reply.Err == ErrNoKey {
			DPrintf("client:%d sends Get to server got no key error: %d, %s \n", ck.clientId, ck.serverId, time.Now().String())
			return ""
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//DPrintf("client: %d sent %s with key:%s val:%s \n", ck.clientId, op, key, value)
	// You will have to modify this function.
	// start from selecting a random server to talk to, until getting an ok.
	ck.clientSeq++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.ClientSeq = ck.clientSeq
	
	for {
		// if this is initial round or last round fail without giving next server to talk to, use a random server.
		if ck.serverId == -1 {
			ck.serverId = int(nrand()) % len(ck.servers)
		}
		reply := PutAppendReply{}
		DPrintf("client:%d sends PA to server: %d, %s \n", ck.clientId, ck.serverId, time.Now().String())
		rpcSuccess := ck.servers[ck.serverId].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("client:%d sends PA to server done: %d, %s \n", ck.clientId, ck.serverId, time.Now().String())
		if !rpcSuccess || reply.Err == ErrWrongLeader{
			ck.serverId = -1
			continue
		}

		// must be OK now, so break.
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
