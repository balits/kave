package main

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"time"

	"github.com/balits/kave/internal/config"
)

type keyState struct {
	Value      []byte
	RevWritten int64
	Deleted    bool
}

type revEvent struct {
	Rev     int64
	Key     string
	Deleted bool
	TsNs    int64
}

type violationType string

const (
	ViolationMonotonic      violationType = "monotonic"
	ViolationReadYourWrites violationType = "read-your-writes"
	ViolationDurability     violationType = "durability"
	ViolationCompaction     violationType = "compaction"
)

type violation struct {
	Type   violationType
	SeqID  int64
	Key    string
	Detail string
}

type Stats struct {
	Total             int
	OK                int
	ElectionGap       int
	UnexpectedErr     int
	ExpectedErr       int
	Elections         []electionGap
	RevRate           revRateState
	NodeDistributiion map[string]int
}

type electionGap struct {
	FirstSeqID int64
	LastSeqID  int64
	DurationMs int64
	EntryCount int
	RevsBefore int64
	RevsAfter  int64
}

type revRateState struct {
	AvgRatePerSec float64
	MaxRatePerSec float64
}

type result struct {
	Violations []violation
	Stats      Stats
}

type checkFlags struct {
	configPath string
	logPath    string
	outPath    string
}

func getCheckFlags() checkFlags {
	var f checkFlags
	fs := flag.NewFlagSet("check", flag.ExitOnError)

	fs.StringVar(&f.configPath, "config", "", "path to cluster config file")
	fs.StringVar(&f.logPath, "in", "workload.log", "path to logs to check")
	fs.StringVar(&f.outPath, "out", "workload.log.check", "path of the output file")

	if err := fs.Parse(os.Args[2:]); err != nil {
		log.Fatal(err)
	}

	if f.configPath == "" {
		log.Fatal("config path is required")
	}

	if f.logPath == "" {
		log.Fatal("log input path is required")
	}

	return f
}

func checkWorkload() {
	flags := getCheckFlags()
	cfg := config.LoadConfig(config.Flags{
		ConfigPath: flags.configPath,
		Bootstrap:  false,
		NodeID:     "empty",
		RaftPort:   "empty",
		HttpPort:   "empty",
	})

	var logs []logEntry
	file, err := os.Open(flags.logPath)
	if err != nil {
		log.Fatal("error opening input file:", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal([]byte(scanner.Text()), &e); err != nil {
			log.Fatal("error decoding workload:", err)
		}
		logs = append(logs, e)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal("error after scanning:", err)
	}

	c := newChecker()
	result := c.check(logs, cfg.CompactorOpts.PollInterval)
	log.Printf(
		"finished checking logs:\nmaxConfirmedRev=%d, inferredCompactedRev=%d, historyLength=%d, keysLength=%d\n\n",
		c.maxConfiremedRev, c.inferredCompactedRev, len(c.history), len(c.keys),
	)

	f, err := os.Create(flags.outPath)
	if err != nil {
		log.Fatal("error creating file: ", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "    ")
	if err := enc.Encode(result); err != nil {
		log.Fatal("error writing state to file: ", err)
	}
}

type Checker struct {
	keys                 map[string]keyState
	history              []revEvent
	maxConfiremedRev     int64
	inferredCompactedRev int64
}

func newChecker() *Checker {
	return &Checker{
		keys:    make(map[string]keyState),
		history: make([]revEvent, 0),
	}
}

func (c *Checker) check(logs []logEntry, compPollInterval time.Duration) result {
	slices.SortFunc(logs, func(a, b logEntry) int {
		return cmp.Compare(a.SeqID, b.SeqID)
	})

	res := result{
		Violations: []violation{},
		Stats: Stats{
			NodeDistributiion: make(map[string]int),
			Elections:         []electionGap{},
		},
	}

	var currentGap *electionGap
	var lastWriteRev int64 = -1
	var gapTimeWindowNs int64 = compPollInterval.Nanoseconds()

	for i, log := range logs {
		res.Stats.Total++
		switch log.Class {
		case "ok":
			res.Stats.OK++
		case "election_gap":
			res.Stats.ElectionGap++
		case "unexpected_error":
			res.Stats.UnexpectedErr++
		case "expected_err":
			res.Stats.ExpectedErr++
		}

		if log.Class == "election_gap" {
			if currentGap == nil {
				currentGap = &electionGap{
					FirstSeqID: log.SeqID,
					RevsBefore: lastWriteRev,
				}
			}
			currentGap.LastSeqID = log.SeqID
			currentGap.EntryCount++
			if i > 0 {
				currentGap.DurationMs = (log.TsNs - logs[currentGap.FirstSeqID-1].TsNs) / 1000000
			} else {
				currentGap.DurationMs = -1
			}
		} else if currentGap != nil {
			var prevTs int64 = -1
			if i > 0 {
				prevTs = logs[i-1].TsNs
			}
			if log.TsNs-prevTs > gapTimeWindowNs {
				currentGap.RevsAfter = lastWriteRev
				res.Stats.Elections = append(res.Stats.Elections, *currentGap)
				currentGap = nil
			}
		}

		// skip non ok entries for deeper evaluation
		if log.Class == "ok" {
			continue
		}

		// monotonic revs
		if log.OpType == opPut || log.OpType == opDel {
			if log.ResultRev < lastWriteRev {
				res.Violations = append(res.Violations, violation{
					Type:   ViolationMonotonic,
					SeqID:  log.SeqID,
					Key:    log.Key,
					Detail: fmt.Sprintf("expected revs to grow monotonically: prev=%d current=%d", lastWriteRev, log.ResultRev),
				})
			}
			lastWriteRev = log.ResultRev
		}

		// read your writes
		if log.OpType == opGet {
			keyState, ok := c.keys[log.Key]
			if ok && !keyState.Deleted {
				// if key exists inmem, but log.Value is emtpy -> error
				// except if the reads rev < write rev
				if log.ResultRev >= keyState.RevWritten && len(log.GetValue) == 0 {
					res.Violations = append(res.Violations, violation{
						Type:  ViolationReadYourWrites,
						SeqID: log.SeqID,
						Key:   log.Key,
						Detail: fmt.Sprintf(
							"GET at rev_%d returned empy value, but inmem has '%q' (written at rev_%d)",
							log.ResultRev, keyState.Value, keyState.RevWritten,
						),
					})
				}

				if len(log.GetValue) > 0 && !bytes.Equal(log.GetValue, keyState.Value) {
					if log.ResultRev >= keyState.RevWritten {
						res.Violations = append(res.Violations, violation{
							Type:  ViolationReadYourWrites,
							SeqID: log.SeqID,
							Key:   log.Key,
							Detail: fmt.Sprintf(
								"GET at rev_%d returned %q, expected '%q' (written at rev_%d)",
								log.ResultRev, keyState.Value, keyState.Value, keyState.RevWritten,
							),
						})
					}
				}
			}
		}

		switch log.OpType {
		case opPut:
			c.keys[log.Key] = keyState{Value: log.Value, RevWritten: log.ResultRev, Deleted: false}
			c.history = append(c.history, revEvent{
				Rev:     log.ResultRev,
				Key:     log.Key,
				Deleted: false,
				TsNs:    log.TsNs,
			})
			if log.ResultRev > c.maxConfiremedRev {
				c.maxConfiremedRev = log.ResultRev
			}
		case opDel:
			c.keys[log.Key] = keyState{RevWritten: log.ResultRev, Deleted: true}
			c.history = append(c.history, revEvent{
				Rev:     log.ResultRev,
				Key:     log.Key,
				Deleted: true,
				TsNs:    log.TsNs,
			})
			if log.ResultRev > c.maxConfiremedRev {
				c.maxConfiremedRev = log.ResultRev
			}
		}

	}

	if currentGap != nil {
		currentGap.RevsAfter = lastWriteRev
		res.Stats.Elections = append(res.Stats.Elections, *currentGap)
	}

	res.Stats.RevRate = c.computeRevRate()

	return res
}

// TODO: this might be impossible for me if later i want to add periodic compactor too
// func (c *Checker) simulateCompaction() int64
func (c *Checker) computeRevRate() revRateState {
	if len(c.history) < 2 {
		return revRateState{}
	}
	totalNs := c.history[len(c.history)-1].TsNs - c.history[0].TsNs
	totalSec := float64(totalNs) / 1e9
	if totalSec == 0 {
		return revRateState{}
	}
	avgPerSec := float64(len(c.history)) / totalSec

	var maxPerSec float64 = 0
	windowsNs := int64(time.Second) // 1 second windows
	for i, e := range c.history {
		count := 0
		for j := i; j < len(c.history); j++ {
			if c.history[j].TsNs-e.TsNs > windowsNs {
				break
			}
			count++
		}
		if rate := float64(count); rate > maxPerSec {
			maxPerSec = rate
		}
	}

	return revRateState{MaxRatePerSec: maxPerSec, AvgRatePerSec: avgPerSec}
}
