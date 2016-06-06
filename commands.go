package main

import (
	"fmt"

	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
)

type RedisClient interface {
	Cmd(string, ...interface{}) *redis.Resp
	//	Close() error
}

type Client struct {
	redis RedisClient
	conn  interface{}
}

func Dial(server string) (rv *Client, err error) {
	var client Client
	var cl *redis.Client
	cl, err = redis.Dial("tcp", server)
	if err != nil {
		fmt.Println(err.Error())
	}
	client.redis = cl
	client.conn = cl
	rv = &client
	return
}

func Cluster(server string) (rv *Client, err error) {
	var client Client
	var cl *cluster.Cluster
	cl, err = cluster.New(server)
	if err != nil {
		fmt.Println(err.Error())
	}
	client.redis = cl
	client.conn = cl
	rv = &client
	return
}

func (c *Client) SendStrRespCommand(cmd string, args ...interface{}) (str string, err error) {
	str, err = c.redis.Cmd(cmd, args).Str()
	return
}

func (c *Client) SendListRespCommand(cmd string, args ...interface{}) (rv []string, err error) {
	rv, err = c.redis.Cmd(cmd, args).List()
	return
}

func (c *Client) SendIntRespCommand(cmd string, args ...interface{}) (rv int, err error) {
	rv, err = c.redis.Cmd(cmd, args).Int()
	return
}

/*func (c *Client) Migrate(ip string, port string, key string, db int, timeout time.Duration) (err error) {*/
func (c *Client) Migrate(ip string, port string, key string, db int, timeout int) (err error) {
	_, err = c.SendStrRespCommand("MIGRATE", ip, port, key, db, timeout)
	return
}

func (c *Client) ClusterGetKeysInSlot(slot, num int) (rv []string, err error) {
	rv, err = c.SendListRespCommand("CLUSTER", "GETKEYSINSLOT", slot, num)
	return
}

func (c *Client) ClusterSetSlotStable(slot int) (err error) {
	_, err = c.SendStrRespCommand("CLUSTER", "SETSLOT", slot, "STABLE")
	return
}

func (c *Client) ClusterSetSlot(slot int, op string, id string) (err error) {
	_, err = c.SendStrRespCommand("CLUSTER", "SETSLOT", slot, op, id)
	return
}

func (c *Client) ClusterAddSlots(slot int) (err error) {
	_, err = c.SendStrRespCommand("CLUSTER", "ADDSLOTS", slot)
	return
}

func (c *Client) ClusterForget(id string) (err error) {
	_, err = c.SendStrRespCommand("CLUSTER", "FORGET", id)
	return
}

func (c *Client) ClusterNodes() (rv string, err error) {
	rv, err = c.SendStrRespCommand("cluster", "nodes")
	return
}

func (c *Client) ClusterReplicate(id string) (err error) {
	_, err = c.SendStrRespCommand("cluster", "replicate", id)
	return
}
func (c *Client) ClusterMeet(ip, port string) (err error) {
	_, err = c.SendStrRespCommand("cluster", "meet", ip, port)
	return
}

func (c *Client) ClusterFailover() (err error) {
	_, err = c.SendStrRespCommand("CLUSTER", "FAILOVER")
	return
}

func (c *Client) ClusterCountKeysInSlot(slot int) (rv int, err error) {
	rv, err = c.SendIntRespCommand("CLUSTER", "COUNTKEYSINSLOT", slot)
	return
}

func (c *Client) Close() {
	if c.conn == nil {
		return
	}
	switch c.conn.(type) {
	case *redis.Client:
		c.conn.(*redis.Client).Close()
	case *cluster.Cluster:
		c.conn.(*cluster.Cluster).Close()
	}
}

/*func main() {
	//var b, c RedisClient
	var t, f Client
	var err error

	//client, err := redis.Dial("tcp", "127.0.0.1:7007")
	t.Redis, err = redis.Dial("tcp", "127.0.0.1:7007")
	f.Redis, err = cluster.New("127.0.0.1:7007")
	//t := Client{Redis: b}
	//f := Client{Redis: c}
	if err != nil {
		fmt.Println(err.Error())
	}
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(t.ClusterNodes())
	fmt.Println(f.ClusterNodes())
	//Replicate(client, "2bad03e1b052471c6552bc22515f71105460821c")
	rv, _ := ClusterCountKeysInSlot(client, 5000)
	fmt.Println(rv)
	l, _ := ClusterGetKeysInSlot(client, 5000, 5)
	for _, v := range l {
		fmt.Println(v)
	}
	//defer b.Close()
	var f *Client
	f = Dial("127.0.0.1:7007")
	//f.Redis, _ = cluster.New("127.0.0.1:7007")
	rv, _ := f.ClusterNodes()
	fmt.Println(rv)
	f.Close()
}
*/
