package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"os"

	"github.com/urfave/cli"
)

var addCommand = cli.Command{
	Name:        "add",
	Aliases:     []string{"-a"},
	Description: fmt.Sprintf("add a node from the redis cluster\n    for example:\n    add a slave node 127.0.0.1:7001 and its master is 127.0.0.1:7002\n        %s add 127.0.0.1:7000 127.0.0.1:7001 --master 127.0.0.1:7002\n    add a master node 127.0.0.1:7001\n        %s add 127.0.0.1:7000 127.0.0.1:7001\n", os.Args[0], os.Args[0]),
	Usage:       "ip:port(connect node) ip:port(delete node)",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "master",
			Value: "",
			Usage: "ip:port",
		},
	},
	Action: func(c *cli.Context) (err error) {
		if len(c.Args()) < 2 {
			fmt.Println("ip:port(connect node) ip:port(delete node)  mut be provied")
			os.Exit(1)
		}
		var node RedisNodes
		myargs := make(map[string]string)
		if c.String("file") != "" {
			myargs["file"] = c.String("file")
		} else {
			myargs["host"] = c.Args().Get(0)
		}
		node.IP = c.Args().Get(1)
		if c.String("master") != "" {
			node.Master = c.String("master")
			node.Type = Slave
		} else {
			node.Type = Master
		}

		rc := RedisTrib(myargs)
		defer rc.CloseAll()
		rc.configNodes[node.IP] = &node
		err = rc.AddHost(node.IP, node.Type)
		return
	},
}

// add a master node
// host string: ip:port => 127.0.0.1:8000
func (r *redisCluster) AddHost(host string, nodeType int) (err error) {
	log.Printf("Add host %s, type %s\n", host, HostType[nodeType])
	if !IsValidIP(host) || nodeType == -1 {
		log.Fatal("invalId host format or node type!")
	}
	hostInfo := strings.Split(host, ":")
	err = r.conn.ClusterMeet(hostInfo[0], hostInfo[1])
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	for i := 0; ; i++ {
		time.Sleep(RETRY_INTERVAL * time.Second)
		if r.CheckAllNode(callbackNodeInList, host) {
			log.Printf("add node %s sucess!\n", host)
			break
		}
		if i >= MAX_RETRY {
			log.Fatal(fmt.Sprintf("add node %s faild", host))
		}
	}
	r.FreshClusterInfo()
	if nodeType == Slave { // if it was a slave node

		node, has := r.configNodes[host]
		if has { //if this node has configuration on the configuration file
			if !IsValidIP(node.Master) { // check the master ip is valId, split with ":" such as "127.0.0.1:8000"
				err = errors.New(fmt.Sprintf("invalId Master config of slave %s", host))
				log.Fatal(err.Error())
				return
			}
			var master *RedisNodes
			master, has = r.configNodes[node.Master]
			if has { //if the slave's master has configuration on the configuration file
				_, has = r.runNodes[node.Master] // check the master node exists on the online environment or not
				if !has {                        // if not exists add it
					log.Printf("Master node %s not exist\n", node.Master)
					if err = r.AddHost(master.IP, master.Type); err != nil {
						log.Fatal(err.Error())
						return
					}
				}
				if r.runNodes[node.Master].Type == Slave { //if the master node was configure as the slave node or not
					err = errors.New(fmt.Sprintf("Node %s is a slave node, can't change it to the master state", node.Master))
					log.Fatal(err.Error())
					return
				}
			} else { // if the master node not found on the json file
				log.Printf("Master node %s not exist!\n", node.IP)
				os.Exit(1)
			}
		}
		master := r.runNodes[node.Master]
		if err = r.replicate(node.IP, master); err != nil { //change the node to the slave Status
			log.Fatal(err.Error())
			return
		}
		r.FreshClusterInfo() //refreash the local node list

	}
	log.Printf("Add %s sucess\n", host)
	return
}
