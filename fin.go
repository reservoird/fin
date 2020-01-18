package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/reservoird/icd"
)

// FinCfg contains config
type FinCfg struct {
	Name          string
	File          string
	Follow        bool
	Poll          string
	SleepDuration string
	Timestamp     bool
}

// FinStats contains stats
type FinStats struct {
	Name             string
	MessagesReceived uint64
	MessagesSent     uint64
	Running          bool
}

// Fin contains what is needed for ingester
type Fin struct {
	cfg    FinCfg
	file   string
	follow bool
	poll   time.Duration
	sleep  time.Duration
	run    bool
}

// New is what reservoird uses to create and start stdin
func New(cfg string) (icd.Ingester, error) {
	c := FinCfg{
		Name:          "com.reservoird.ingest.fin",
		File:          "test.txt",
		Follow:        false,
		Poll:          "10s",
		SleepDuration: "1ms",
		Timestamp:     false,
	}
	if cfg != "" {
		d, err := ioutil.ReadFile(cfg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(d, &c)
		if err != nil {
			return nil, err
		}
	}
	sleep, err := time.ParseDuration(c.SleepDuration)
	if err != nil {
		return nil, fmt.Errorf("error parsing sleep duration")
	}
	poll, err := time.ParseDuration(c.Poll)
	if err != nil {
		return nil, fmt.Errorf("error parsing poll amount")
	}
	o := &Fin{
		cfg:    c,
		file:   c.File,
		follow: c.Follow,
		poll:   poll,
		sleep:  sleep,
		run:    false,
	}
	return o, nil
}

// Name return the name of the Ingester
func (o *Fin) Name() string {
	return o.cfg.Name
}

// Running states wheter or not ingest is running
func (o *Fin) Running() bool {
	return o.run
}

// Ingest reads data from stdin and writes it to the queue
func (o *Fin) Ingest(queue icd.Queue, mc *icd.MonitorControl) {
	defer mc.WaitGroup.Done() // required

	stats := FinStats{}
	stats.Name = o.cfg.Name
	stats.Running = o.run

	f, err := os.Open(o.file)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fsize, err := os.Stat(o.file)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	reader := bufio.NewReader(f)

	o.run = true
	for o.run == true {
		line, err := reader.ReadString('\n')
		if err == nil {
			stats.MessagesReceived = stats.MessagesReceived + 1
			if queue.Closed() == false {
				if len(line) != 0 {
					if o.cfg.Timestamp == true {
						line = fmt.Sprintf("[%s %s] ", o.Name(), time.Now().Format(time.RFC3339)) + line
					}
					err = queue.Put([]byte(line))
					if err != nil {
						fmt.Printf("%v\n", err)
					} else {
						stats.MessagesSent = stats.MessagesSent + 1
					}
				}
			}
		} else {
			if err == io.EOF {
				if o.follow == true {
					usize, err := os.Stat(o.file)
					if err != nil {
						fmt.Printf("%v\n", err)
					} else {
						if usize.Size() < fsize.Size() {
							f.Close()
							f, err = os.Open(o.file)
							if err != nil {
								fmt.Printf("%v\n", err)
							}
							reader = bufio.NewReader(f)
							fsize = usize
						}
					}
				}
				select {
				case <-mc.DoneChan:
					o.run = false
					stats.Running = o.run
					continue
				case <-time.After(o.poll):
				}
			} else {
				fmt.Printf("%v\n", err)
			}
		}

		// clear stats
		select {
		case <-mc.ClearChan:
			stats = FinStats{
				Name:    o.cfg.Name,
				Running: o.run,
			}
		default:
		}

		// send stats
		select {
		case mc.StatsChan <- stats:
		default:
		}

		// listens for shutdown
		select {
		case <-mc.DoneChan:
			o.run = false
			stats.Running = o.run
		case <-time.After(o.sleep):
		}
	}

	// close before exiting
	f.Close()

	// send final stats blocking
	mc.FinalStatsChan <- stats
}
