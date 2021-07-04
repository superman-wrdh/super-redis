package core

import (
	"strings"
	"super-redis/datastruct/set"
	"super-redis/interface/redis"
	"super-redis/redis/reply"
)

var forbiddenInMulti = set.Make(
	"flushdb",
	"flushall",
)

// EnqueueCmd puts command line into `multi` pending queue
func EnqueueCmd(db *DB, conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if forbiddenInMulti.Has(cmdName) {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if cmd.prepare == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		// difference with redis: we won't enqueue command line with wrong arity
		return reply.MakeArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return reply.MakeQueuedReply()
}
