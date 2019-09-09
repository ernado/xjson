package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	j "github.com/json-iterator/go"
)

type Debtor struct {
	Companies []string
	Phones    []string
	Debt      int64
}

type Aggregator struct {
	sync.Mutex
	count         int
	start         time.Time
	debtors       []Debtor
	debtorByPhone map[string]int
}

func (a *Aggregator) PrintResult() {
	a.Lock()
	defer a.Unlock()

	fmt.Println("total:", a.count)
	fmt.Println("total debtors:", len(a.debtors))
	fmt.Println("total time:", time.Since(a.start))
	for _, d := range a.debtors {
		sort.Strings(d.Companies)
		sort.Strings(d.Phones)

		fmt.Printf("%s:\n", strings.Join(d.Companies, ", "))
		fmt.Printf("\t%s\n", strings.Join(d.Phones, ", "))
		fmt.Printf("\t%d\n", d.Debt)
	}
}

func (a *Aggregator) Process(r WorkerResult) {
	a.Lock()
	a.count++
	var (
		id    int
		found bool
	)
	for _, p := range r.Phones {
		id, found = a.debtorByPhone[p]
		if found {
			break
		}
	}
	if !found {
		// Creating new company.
		a.debtors = append(a.debtors, Debtor{
			Companies: []string{r.Company},
			Phones:    append([]string{}, r.Phones...),
		})
		id = len(a.debtors) - 1
		for _, p := range r.Phones {
			a.debtorByPhone[p] = id
		}
	}
	nameFound := false
	for _, name := range a.debtors[id].Companies {
		if name == r.Company {
			nameFound = true
			break
		}
	}
	if !nameFound {
		a.debtors[id].Companies = append(a.debtors[id].Companies, r.Company)
	}
	for _, p := range r.Phones {
		found := false
		for _, pp := range a.debtors[id].Phones {
			if pp == p {
				found = true
				break
			}
		}
		if !found {
			a.debtorByPhone[p] = id
			a.debtors[id].Phones = append(a.debtors[id].Phones, p)
		}
	}
	a.debtors[id].Debt += r.Debt
	a.Unlock()
}

var (
	cpuprofile = flag.String("cpu.out", "", "write cpu profile to file")
	memprofile = flag.String("mem.out", "", "write cpu profile to file")
	fileName   = flag.String("f", "1.json", "file name to open")
)

func main() {
	flag.Parse()
	t := time.Now()

	// Profiling.
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	f, err := os.Open(*fileName)
	if err != nil {
		log.Fatalln("failed to open file:", err)
	}
	cfg := j.ConfigDefault
	a := &Aggregator{
		debtors:       make([]Debtor, 0, 100),
		debtorByPhone: make(map[string]int, 100),
		start:         t,
	}
	p := &Robin{}
	for i := 0; i < 6; i++ {
		w := &RobinWorker{
			Running: true,
			I:       j.NewIterator(cfg),
			F:       a.Process,
		}
		p.total++
		p.workers = append(p.workers, w)
		go w.Run()
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		buf := s.Bytes()
		if bytes.Contains(buf, []byte(`{`)) {
			p.Schedule(buf)
		}
	}
	p.Stop()
	a.PrintResult()

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
