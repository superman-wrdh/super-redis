package core

import (
	"fmt"
	"runtime/debug"
	"strings"
	"super-redis/interface/redis"
	"super-redis/lib/logger"
	"super-redis/redis/reply"
)

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// authenticate
	if cmdName == "auth" {
		return Auth(db, c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}

	// special commands
	done := false
	result, done = execSpecialCmd(c, cmdLine, cmdName, db)
	if done {
		return result
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(db, c, cmdLine)
	}

	// normal commands
	return execNormalCommand(db, cmdLine)
}

func execNormalCommand(db *DB, cmdArgs [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdArgs[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdArgs) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdArgs[1:])
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdArgs[1:])
}

func execSpecialCmd(c redis.Connection, cmdLine [][]byte, cmdName string, db *DB) (redis.Reply, bool) {
	// TODO
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return reply.MakeArgNumErrReply("subscribe"), true
		}
		//TODO
	}
	return nil, true
}
