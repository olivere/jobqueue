package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/olivere/jobqueue"
	"github.com/olivere/jobqueue/mysql"
	"github.com/olivere/jobqueue/ui/server"
)

func main() {
	const (
		exampleDBURL = "root@tcp(127.0.0.1:3306)/jobqueue_e2e?loc=UTC&parseTime=true"
	)
	var (
		addr    = flag.String("addr", "127.0.0.1:12345", "HTTP bind address")
		dburl   = flag.String("dburl", "", "MySQL dsn for persistent storage, e.g. "+exampleDBURL)
		dbdebug = flag.Bool("dbdebug", false, "Enabled debug output for DB store")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Initialize the manager
	var options []jobqueue.ManagerOption
	if *dburl != "" {
		var dboptions []mysql.StoreOption
		if *dbdebug {
			dboptions = append(dboptions, mysql.SetDebug(true))
		}
		store, err := mysql.NewStore(*dburl, dboptions...)
		if err != nil {
			log.Fatal(err)
		}
		options = append(options, jobqueue.SetStore(store))
	}
	m := jobqueue.New(options...)
	defer m.Close()

	errc := make(chan error, 1)

	go func() {
		log.Printf("web server listening on %v", *addr)
		s := server.New(m)
		errc <- s.Serve(*addr)
	}()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		log.Printf("recv signal %v", fmt.Sprint(<-c))
		errc <- nil
	}()

	if err := <-errc; err != nil {
		log.Printf("exit with error %v", err)
		os.Exit(1)
	}
}
