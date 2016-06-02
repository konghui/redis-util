package nodes

import (
	"gopkg.in/redis.v3"
)

type RedisNodes struct {
	IP     string
	Type   int
	Master string
	Slot   [][]int
	Status int
	Id     string
	Client *redis.Client
}

// get the connect from the runNode
func (n *RedisNodes) Connect() (conn *redis.Client) {

	if n.Client == nil { // has no redisClient is nil, try to connect
		n.Client = redis.NewClient(&redis.Options{Addr: n.IP})
		conn = n.Client
	}
	conn = n.Client
	return
}
