package core

import (
	"super-redis/config"
	"super-redis/interface/redis"
	"super-redis/redis/reply"
)

// Auth validate client's password
func Auth(db *DB, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.RequirePass == "" {
		return reply.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Properties.RequirePass != passwd {
		return reply.MakeErrReply("ERR invalid password")
	}
	return &reply.OkReply{}
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}
