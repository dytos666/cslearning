package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name_    string
	mystate_ string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name_ = l
	lk.mystate_ = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	flag := false
	for {
		indetify, version, err := lk.ck.Get(lk.name_)
		if flag && indetify == lk.mystate_ {
			break
		}
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.name_, lk.mystate_, version)
			if err == rpc.OK {
				break
			} else if err == rpc.ErrMaybe {
				flag = true
				continue
			}
		} else if indetify == "" {
			err = lk.ck.Put(lk.name_, lk.mystate_, version)
			if err == rpc.OK {
				break
			} else if err == rpc.ErrMaybe {
				flag = true
				continue
			}
		}

	}

}

func (lk *Lock) Release() {
	for {
		indetify, version, err := lk.ck.Get(lk.name_)

		if err == rpc.OK && indetify == lk.mystate_ {
			err = lk.ck.Put(lk.name_, "", version)
			if err == rpc.OK || err == rpc.ErrMaybe {
				break
			}
		}
	}
}
