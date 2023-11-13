package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Debuger struct {
	NodeId     int
	Term       *int
	DebugStart time.Time
	allflag    bool
	ElectFlag  bool
	VoteFlag   bool
	LeaderFlag bool
	LogFlag    bool
	HeartFlag  bool
	logFile    *os.File
	err        error
}

func (debuger *Debuger) InitDebuger(NodeId int, Term *int) {
	debuger.logFile, debuger.err = os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	debuger.DebugStart = time.Now()
	debuger.Term = Term
	debuger.NodeId = NodeId
	debuger.ElectFlag = true
	debuger.LeaderFlag = true
	debuger.VoteFlag = true
	debuger.LogFlag = true
	debuger.HeartFlag = false
	debuger.allflag = false
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	// log.SetOutput(debuger.logFile)
}

func (debuger *Debuger) Debug(format string, flag bool, Type string, a ...interface{}) {
	if flag && debuger.allflag {
		time := time.Since(debuger.DebugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %s S%d Term%d", time, Type, debuger.NodeId, *debuger.Term)
		format = prefix + format
		log.Printf(format, a...)
	}
}
