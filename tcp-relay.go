package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cjkreklow/tcp-relay-pub-vrs/relay"
	"github.com/cjkreklow/tcp-relay-pub-vrs/stream"
	"github.com/dustin/go-humanize"
)

var config = new(struct {
	hostName   string
	portNum    string
	outportNum string
	reconMax   int
	reconDelay time.Duration
})

var exitChannel = make(chan bool)

// initialize command line and exit handling
func init() {
	var rMax = flag.Int("r", 0, "number of reconnect `attempts`")
	var rDelay = flag.Int("d", 10, "reconnect delay (in `seconds`)")
	flag.Parse()

	var args = flag.Args()
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: tcp-relay-pub-vrs <hostname> <hostport> <relayport>")
		os.Exit(1)
	}

	config.hostName = args[0]
	config.portNum = args[1]
	config.outportNum = args[2]
	config.reconMax = *rMax
	config.reconDelay = time.Duration(*rDelay) * time.Second

	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go signalExitWatcher(ch)
}

func main() {
	var err error

	// get relay server
	var server = relay.NewServer(config.outportNum)

	// start stream client
	var client = new(stream.StreamClient)
	client.ReconnectMax = config.reconMax
	client.ReconnectDelay = config.reconDelay
	err = client.Connect(config.hostName, config.portNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting upstream client: %s\n", err)
		os.Exit(1)
	}
	go func() {
		<-exitChannel
		client.Close()
		exit(-1)
	}()

	// start relay server and client receiver, discard messages if queue
	// is full
	err = server.Start(client.Scan(5, true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting relay: %s\n", err)
		os.Exit(1)
	}
	go func() {
		<-exitChannel
		server.Close()
		exit(-1)
	}()

	// start stat printer
	go runtimeStats(server)

	// wait for relay server to exit for some reason
	server.Wait()

	// exit here
	exit(-1)
	os.Exit(exitVal)
}

// background goroutine to watch for OS exit signals and shutdown
func signalExitWatcher(c chan os.Signal) {
	// start graceful shutdown
	select {
	case <-c:
	case <-exitChannel:
	}

	exit(0)

	// second signal forces exit
	<-c
	os.Exit(2)
}

// stat printer
func runtimeStats(server *relay.Server) {
	var m runtime.MemStats
	var stats relay.Statistics
	var statTick = time.NewTicker(5 * time.Second)
	var e error
	var a, ta uint64
	var indent = func(a ...interface{}) {
		fmt.Println(append([]interface{}{"\t"}, a...)...)
	}
	for {
		select {
		case e = <-server.ClientErrors:
			fmt.Println(e)
			e = nil
		case <-statTick.C:
			stats = server.Stats()
			time.Sleep(5 * time.Second)
			indent(" =========  ==========  ==========")
			indent(config.portNum, " -  # goroutines: ", runtime.NumGoroutine(), " Client Connections: ", stats.ClientsActive)
			runtime.ReadMemStats(&m)
			a = m.Mallocs - ta
			ta = m.Mallocs
			indent("Memory Acquired: ", humanize.Bytes(m.Sys))
			indent("Recent Allocs: ", a)
			indent("Last GC: ", m.LastGC)
			indent("Next GC: ", humanize.Bytes(m.NextGC), "Heap Alloc: ", humanize.Bytes(m.HeapAlloc))
			indent(" =========  ==========  ==========")
			fmt.Printf("%+v\n", stats)
		}
	}
}

// exitOnce and exitVal should only be used inside the exit function
// below.  This ensures that we're only closing the exit channel once,
// and only the first call to exit sets the exit value.
var exitOnce = new(sync.Once)
var exitVal int

// exit should be called on all exit paths to guarantee that the exit
// channel is closed and all goroutines have a chance to exit cleanly.
// The return value on the first call to exit will be used in the final
// call to os.Exit().
func exit(val int) {
	exitOnce.Do(func() {
		exitVal = val
		close(exitChannel)
		go func() {
			time.Sleep(30 * time.Second)
			fmt.Println("exit timeout")
			os.Exit(-2)
		}()
	})
}
