package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := Args{}
		args.Wanned()

		reply := Reply{}
		ok := call("Coordinator.RpcHandler", &args, &reply)
		if reply.Done {
			DPrintf("[W]", ColorGreen, "all tasks done, exiting\n")
			return
		}
		if !ok {
			DPrintf("[W]", ColorRed, "wanna call failed!\n")
			continue
		}

		// do mapf
		func() {
			if reply.Flag == 1 {
				args.Flag = 1
				args.MapId = reply.MapId
				filename := reply.Filename
				file, err := os.Open(filename)
				if err != nil {
					args.Failed()
					DPrintf("[W]", ColorRed, "cannot open %v\n", filename)
					goto End
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					args.Failed()
					DPrintf("[W]", ColorRed, "cannot read %v\n", filename)
					goto End
				}
				file.Close()
				kva := mapf(filename, string(content))

				// for j := 0; j < nReduce; j++ {
				// 	oname := fmt.Sprintf("mr-%d-%d", i, j)
				// 	ofile, err := os.Create(oname)
				// 	if err != nil {
				// 		panic(fmt.Sprintf("cannot create file %v: %v", oname, err))
				// 	} else {
				// 		ofile.Close()
				// 	}
				// }

				taskId := reply.MapId
				fileFds := make([]*json.Encoder, reply.NReduce)
				tmpFiles := make([]*os.File, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					tmpfile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*.tmp", taskId, i))

					if err != nil {
						panic(fmt.Sprintf("cannot create file %v: %v", tmpfile, err))
					}
					tmpFiles[i] = tmpfile
					enc := json.NewEncoder(tmpfile)
					fileFds[i] = enc
				}

				for _, kv := range kva {
					hash_key := ihash(kv.Key) % reply.NReduce
					err := fileFds[hash_key].Encode(&kv)
					if err != nil {
						args.Failed()
						DPrintf("[W]", ColorRed, "json encode failed!\n")
						goto End
					}
				}

				for i := 0; i < reply.NReduce; i++ {
					tmpFiles[i].Close()

					oname := fmt.Sprintf("mr-%d-%d", taskId, i)
					if err := os.Rename(tmpFiles[i].Name(), oname); err != nil {
						panic(fmt.Sprintf("cannot rename %v to %v: %v", tmpFiles[i].Name(), oname, err))
					}
				}
				args.Succeed()
				goto End

			} else if reply.Flag == 2 { // do reducer

				args.Flag = 2
				args.ReduceId = reply.ReduceId
				tmpfile, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d.tmp", reply.ReduceId))
				if err != nil {
					args.Failed()
					DPrintf("[W]", ColorRed, "cannot create reduce output %v\n", tmpfile)
					goto End
				}
				kva := []KeyValue{}
				for i := 0; i < reply.NTask; i++ {
					// if reply.ReduceId == 0 {
					// 	DPrintf("[W]", ColorRed, "cannot create reduce output \n")
					// }
					oname := fmt.Sprintf("mr-%d-%d", i, reply.ReduceId)
					ofile, err := os.Open(oname)
					if err != nil {
						panic(fmt.Sprintf("cannot open intermediate file %v: %v", oname, err))
					}
					dec := json.NewDecoder(ofile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
					ofile.Close()
				}

				sort.Sort(ByKey(kva))
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)
					fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				tmpfile.Close()
				oname := fmt.Sprintf("mr-out-%d", reply.ReduceId)
				//DPrintf("[W]", ColorRed, "cannot create reduce output %v\n", oname)
				if err := os.Rename(tmpfile.Name(), oname); err != nil {
					panic(fmt.Sprintf("cannot rename %v to %v: %v", tmpfile.Name(), oname, err))
				}
			}

			args.Succeed()
			goto End

		End:
			ok = call("Coordinator.RpcHandler", &args, &reply)
			if !ok {
				DPrintf("[W]", ColorRed, "reply call failed!\n")
				return
			}
		}()
	}
}

// send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	DPrintf("[W]", ColorRed, "rpc call error: %v\n", err)
	return false
}
