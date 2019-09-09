package main

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	j "github.com/json-iterator/go"
	jsoniter "github.com/json-iterator/go"
)

type WorkerResult struct {
	Company string
	Debt    int64
	Phones  []string
}

type RobinWorker struct {
	sync.Mutex
	total   uint64
	Buf     []byte
	F       func(result WorkerResult)
	I       *jsoniter.Iterator
	Running bool
	Closed  chan struct{}
}

func (w *RobinWorker) Stop() uint64 {
	w.Lock()
	w.Running = false
	w.Unlock()
	<-w.Closed
	return atomic.LoadUint64(&w.total)
}

func (w *RobinWorker) Run() {
	var (
		company string
		debt    int64
		buf     []byte
	)
	phones := make([]string, 0, 10)
	i := w.I
	w.Closed = make(chan struct{})

	defer func() {
		w.Closed <- struct{}{}
	}()

	for {
		w.Lock()
		if !w.Running {
			w.Unlock()
			break
		}

		buf = append(buf[:0], w.Buf...)
		w.Buf = w.Buf[:0]

		if len(buf) == 0 {
			w.Unlock()
			continue
		}

		phones = phones[:0]
		i.ResetBytes(buf)
		i.ReadMapCB(func(i *j.Iterator, k string) bool {
			switch k {
			case "company":
				switch i.WhatIsNext() {
				case j.StringValue:
					company = i.ReadString()
				case j.ObjectValue:
					i.ReadMapCB(func(i *j.Iterator, s string) bool {
						company = i.ReadString()
						return true
					})
				default:
					i.Skip()
				}
			case "debt":
				switch i.WhatIsNext() {
				case j.NumberValue:
					debt = i.ReadInt64()
				case j.StringValue:
					v := i.ReadString()
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						panic(err)
					}
					debt = n
				default:
					i.Skip()
				}
			case "phones", "phone":
				switch i.WhatIsNext() {
				case j.StringValue:
					phones = append(phones, i.ReadString())
				case j.NumberValue:
					phones = append(phones, i.ReadNumber().String())
				case j.ArrayValue:
					i.ReadArrayCB(func(i *j.Iterator) bool {
						switch i.WhatIsNext() {
						case j.StringValue:
							phones = append(phones, i.ReadString())
						case j.NumberValue:
							phones = append(phones, i.ReadNumber().String())
						default:
							i.Skip()
						}
						return true
					})
				default:
					i.Skip()
				}
			default:
				i.Skip()
			}
			return true
		})
		atomic.AddUint64(&w.total, 1)

		w.F(WorkerResult{
			Company: company,
			Phones:  phones,
			Debt:    debt,
		})
		w.Unlock()
	}
}

type Robin struct {
	current   uint32
	total     uint32
	processed uint64
	workers   []*RobinWorker
}

func (r *Robin) Schedule(buf []byte) {
	atomic.AddUint64(&r.processed, 1)
	i := atomic.AddUint32(&r.current, 1) % r.total
	w := r.workers[i]

	w.Lock()
	w.Buf = append(w.Buf[:0], buf...)
	w.Unlock()
}

func (r *Robin) Stop() {
	var result uint64
	for _, w := range r.workers {
		result += w.Stop()
	}
	fmt.Println("stopped", atomic.LoadUint64(&r.processed), result)
}
