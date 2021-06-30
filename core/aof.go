package core

import (
	"io"
	"os"
	"strings"
	"super-redis/lib/logger"
	"super-redis/redis/parser"
	"super-redis/redis/reply"
	"super-redis/utils"
)

// loadAof read aof file
func (db *DB) loadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := db.aofChan
	db.aofChan = nil
	defer func(aofChan chan *reply.MultiBulkReply) {
		db.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	reader := utils.NewLimitedReader(file, maxBytes)
	ch := parser.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		cmd := strings.ToLower(string(r.Args[0]))
		command, ok := cmdTable[cmd]
		if ok {
			handler := command.executor
			handler(db, r.Args[1:])
		}
	}
}
