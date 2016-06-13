package main

import (
	"errors"
	"fmt"
	"log"

	"os"

	"github.com/urfave/cli"
)

var restoreCommand = cli.Command{
	Name:        "restore",
	Aliases:     []string{"-r"},
	Description: fmt.Sprintf("restore the redis cluster status from json config\n   for example:\n        %s restore config.json", os.Args[0]),
	Usage:       "config.json",
	ArgsUsage:   "FILE.",
	Action: func(c *cli.Context) (err error) {
		if len(c.Args()) < 1 {
			fmt.Println("Json file must be provide")
			return
		}
		myargs := make(map[string]string)
		myargs["file"] = c.Args().First()
		rc := RedisTrib(myargs)
		defer rc.CloseAll()
		err = rc.Restore()

		return

	},
}

// fix the cluster
func (r *redisCluster) Restore() (err error) {
	log.Println("Delete the disconnect node from cluster.....")
	r.RemoveDisconnectNode()
	log.Println("Try to check and fix the cloud........")
	r.SetAllNodeType()
	log.Println("check the slot in the node....")
	r.SetAllNodeSlot()
	return
}

func (r *redisCluster) RemoveDisconnectNode() {
	for _, node := range r.runNodes {
		if node.Status == DisConnected {
			r.ForgetAll(node)
		}
	}
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
