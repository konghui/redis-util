package main

import (
	"fmt"
	"log"
	"strings"
)

type RedisNodes struct {
	IP     string
	Type   int
	Master string
	Slot   [][]int
	Status int
	Id     string
	Client *Client
}

// get the connect from the runNode
func (n *RedisNodes) Connect() (conn *Client) {
	var err error

	if n.Client == nil { // has no redisClient is nil, try to connect
		n.Client, err = Dial(n.IP)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	conn = n.Client
	return
}

// copy configure to the nodes
func (r *redisCluster) registerNodes(node *RedisNodes) {
	r.configNodes[node.IP] = node
}

func (r *redisCluster) initNodes() {
	for i := 0; i < len(r.Schema); i++ {
		r.configNodes[r.Schema[i].IP] = &r.Schema[i]

	}
	r.ConnectAll()
	r.FreshClusterInfo()

	for ip, _ := range r.runNodes { // make sure runNode and configNode both has same nodes, otherwise if I found a host in the runNode called ip, it may cause nil pointer error when I use configNode[ip].Connect()
		if _, has := r.configNodes[ip]; !has {
			r.configNodes[ip] = &RedisNodes{IP: ip}
		}
	}
}

func (n *RedisNodes) String() (str string) {
	var slot string
	slots := make([]string, 0)
	for _, num := range n.Slot {
		if len(num) == 2 {
			slot = fmt.Sprintf("[%d-%d]", num[0], num[1])
		} else if len(num) == 1 {
			slot = fmt.Sprintf("[%d]", num[0])
		} else {
			log.Fatal("invaliad slot %s", num)
		}
		slots = append(slots, slot)
	}
	str = fmt.Sprintf("IP=>%s\tMaster=>%s\tType=%s\tId=%s\tStatus=>%s\tSlot=>%sClient=>%p", n.IP, n.Master, HostType[n.Type], n.Id, ConnStatus[n.Status], strings.Join(slots, ","), n.Client)
	return
}

func nodeStatus(nodes map[string]*RedisNodes) {
	for _, v := range nodes {
		fmt.Println(v)
	}
}

func callbackNodeInList(node *RedisNodes, args interface{}) bool {
	if node.Type == Master {
		return true
	}
	val, err := node.Connect().ClusterNodes()
	if err != nil {
		log.Fatal(err.Error())
		return false
	}
	fmt.Println(val)
	if strings.Contains(val, args.(string)) {
		return true
	}
	return false
}

// run the callback function on the all node
func (r *redisCluster) CheckAllNode(callback func(*RedisNodes, interface{}) bool, args interface{}) bool {
	for _, redisNode := range r.configNodes {
		if !callback(redisNode, args) {
			return false
		}
	}
	return true
}
