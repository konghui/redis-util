package main

import (
	"errors"
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

func (r *redisCluster) SetAllNodeType() (err error) {
	for _, node := range r.configNodes {
		if err = r.SetNode(node); err != nil {
			log.Fatal(err.Error())
		}
	}
	r.FreshClusterInfo()
	return
}

func (r *redisCluster) SetAllNodeSlot() (err error) {
	for _, node := range r.configNodes {
		r.SetNodeSlot(node)
	}
	r.FreshClusterInfo()
	return
}

func (r *redisCluster) SetNode(nodeInfo *RedisNodes) (err error) {
	_, has := r.runNodes[nodeInfo.IP] //check node exists or not
	if !has {
		log.Printf("node %s not exists! Add it\n", nodeInfo.IP)
		if err = r.AddHost(nodeInfo.IP, nodeInfo.Type); err != nil {
			return
		}

	}
	if err = r.SetNodeType(nodeInfo); err != nil {
		return
	}
	r.SetNodeSlot(nodeInfo)
	return
}

func (r *redisCluster) SetNodeSlot(nodeinfo *RedisNodes) {
	if len(nodeinfo.Slot) == 0 {
		return
	}
	runinfo := r.runNodes[nodeinfo.IP]
	for _, slotDst := range nodeinfo.Slot {
		fromDst, toDst := getSlotInfo(slotDst)
		for i := fromDst; i <= toDst; {
			nodeSrc, end := r.getNodeBySlot(i, toDst)
			if nodeSrc == nil {
				r.AddSlot(i)
				i++
				continue
			}
			r.FreshClusterInfo()
			for j := i; j <= end; j++ {
				r.MoveSlot(nodeSrc, runinfo, j)
			}
			i = end + 1
		}
	}
}

func (r *redisCluster) getNodeBySlot(slot int, max int) (find *RedisNodes, end int) {
	for _, node := range r.runNodes { // if it's a slave node and has no slot skip it
		if node.Type == Slave || len(node.Slot) == 0 {
			continue
		}
		for _, slotInfo := range node.Slot {
			from, to := getSlotInfo(slotInfo)
			if from <= slot && slot <= to {
				find = node
				if to >= max {
					end = max
				} else {
					end = to
				}
				return
			}
		}
	}
	return
}

func (r *redisCluster) SetNodeType(nodeInfo *RedisNodes) (err error) {
	runInfo := r.runNodes[nodeInfo.IP]
	if runInfo.Type == nodeInfo.Type { // if config node type equal runenv node type
		log.Printf("node %s is in the correct state config.type=%s  run.type=%s\n", nodeInfo.IP, HostType[nodeInfo.Type], HostType[runInfo.Type])
		return
	}
	log.Printf("node %s is in an incorrect state, try to fix it\n", nodeInfo.IP)
	if nodeInfo.Type == Master {
		if err = r.FailOver(nodeInfo.IP); err != nil {
			return
		}
	} else if nodeInfo.Type == Slave {
		master, has := r.runNodes[nodeInfo.Master]
		if !has {
			log.Fatal(fmt.Sprintf("can't find node %s Master node %s.", nodeInfo.IP, nodeInfo.Master))
		} // if master is slave fix
		if master.Type == Slave {
			if err = r.FailOver(master.IP); err != nil {
				return
			}
		} else {
			if err = r.replicate(nodeInfo.IP, master); err != nil {
				return
			}
		}
	} else {
		err = errors.New(fmt.Sprintf("unknown type %s", HostType[nodeInfo.Type]))
	}
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

func nodeStatus(nodes map[string]*RedisNodes) {
	var slot string
	slots := make([]string, 0)
	for _, v := range nodes {
		for _, num := range v.Slot {
			if len(num) == 2 {
				slot = fmt.Sprintf("[%d-%d]", num[0], num[1])
			} else if len(num) == 1 {
				slot = fmt.Sprintf("[%d]", num[0])
			} else {
				log.Fatal("invaliad slot %s", num)
			}
			slots = append(slots, slot)
		}
		log.Printf("IP=>%s\tMaster=>%s\tType=%s\tId=%s\tStatus=>%s\tSlot=>%s", v.IP, v.Master, HostType[v.Type], v.Id, ConnStatus[v.Status], strings.Join(slots, ","))
		fmt.Println()
	}
}

func callbackNodeInList(node *RedisNodes, args interface{}) bool {
	if node.Type == Master {
		return true
	}
	val, err := node.Client.ClusterNodes()
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
