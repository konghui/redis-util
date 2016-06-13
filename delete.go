package main

import (
	"fmt"

	"os"

	"github.com/urfave/cli"
)

var deleteCommand = cli.Command{
	Name:        "delete",
	Aliases:     []string{"-d"},
	Description: fmt.Sprintf("delete a node from the redis cluster\n    for example:\n    delete 127.0.0.1:7000 from the cluster use connect 127.0.0.1:70001\n        %s delete 127.0.0.1:7000 127.0.0.1:7001\n    delete 127.0.0.1:7000 through schema file config.json\n        %s delete 127.0.0.1:7000 --file config.json\n    delete node 127.0.0.1:7000 through host 127.0.0.1:7002\n        %s 127.0.0.1:7000 --host 127.0.0.1:7002", os.Args[0], os.Args[0], os.Args[0]),
	Usage:       "ip:port",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "file",
			Value: "",
			Usage: "snapshot.json",
		},

		cli.StringFlag{
			Name:  "host",
			Value: "",
			Usage: "ip:port",
		},
	},
	Action: func(c *cli.Context) (err error) {

		if len(c.Args()) < 1 {
			fmt.Println("ip:port(delete node)  mut be provied")
		}
		myargs := make(map[string]string)
		if len(c.Args()) >= 2 {
			myargs["host"] = c.Args().Get(1)
		} else if c.String("host") != "" {
			myargs["host"] = c.String("host")
		} else if c.String("file") != "" {
			myargs["file"] = c.String("file")
		} else {
			fmt.Printf("Please give the cluster --host ip:port or --file config.json\n")
			return
		}
		rc := RedisTrib(myargs)
		defer rc.CloseAll()
		fmt.Printf("delete node %s from the cluster\n", c.Args().First())
		err = rc.ForgetAll(rc.runNodes[c.Args().First()])
		return
	},
}

func (r *redisCluster) forget(host string, Id string) (err error) {
	conn := r.configNodes[host].Connect()
	err = conn.ClusterForget(Id)
	return
}

// do forget on the all the node, queal delete the node from the cluster
func (r *redisCluster) ForgetAll(forgetHost *RedisNodes) (err error) {
	for host := range r.runNodes {
		if forgetHost.IP == host { // node can not forget itself
			continue
		}
		if err = r.forget(host, forgetHost.Id); err != nil {
			return
		}

	}
	r.FreshClusterInfo()
	return
}
