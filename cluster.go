package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	MAX_RETRY      = 10
	RETRY_INTERVAL = 10
)
const (
	Master = iota
	Slave
	Handshake
)

const (
	DisConnected = iota
	Connected
)

var ConnStatus = [...]string{
	"disconnected",
	"connected",
}

var HostType = [...]string{
	"master",
	"slave",
	"handshake",
}

type redisCluster struct {
	Schema      []RedisNodes
	logger      *log.Logger
	Logfile     string
	configNodes map[string]*RedisNodes
	conn        *Client
	runNodes    map[string]*RedisNodes
}

func (r *redisCluster) ConfigParser(configfile string) (err error) {
	file, err := os.Open(configfile)
	if err != nil {
		return
	}
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(r); err != nil {
		return
	}
	return
}

func (r *redisCluster) ParseFromFile(configfile string) {

	if err := r.ConfigParser(configfile); err != nil {
		log.Fatal(err.Error())
	}
	if r.Logfile != "" {
		fd, err := os.OpenFile(r.Logfile, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err.Error())
			os.Exit(1)
		}
		r.logger = log.New(fd, "redis_snapshot", log.Ldate|log.Ltime|log.Lshortfile)
	}

}

func (r *redisCluster) ParseFromHost(host string) {
	FreshThroughHost(host, r.configNodes)
}

func RedisTrib(args map[string]string) (r *redisCluster) {
	var trib redisCluster
	trib.configNodes = make(map[string]*RedisNodes)
	trib.runNodes = make(map[string]*RedisNodes)
	if val, has := args["file"]; has {
		trib.ParseFromFile(val)
	} else if val, has := args["host"]; has {
		trib.ParseFromHost(val)
	}
	trib.initNodes()
	return &trib
}

func (r *redisCluster) ShowConfig() {
	log.Println("Config INFO:")
	nodeStatus(r.configNodes)
}

func (r *redisCluster) ShowRun() {
	log.Println("Running Info:")
	nodeStatus(r.runNodes)
}

// connect to the all server in the list
func (r *redisCluster) ConnectAll() {
	list := make([]string, len(r.configNodes))
	for _, v := range r.configNodes {
		v.Connect()
		if r.conn == nil {
			var err error
			r.conn, err = Cluster(v.IP)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}
	if r.conn == nil {
		log.Fatal("faild to connect %s\n", list)
	}
}

func (r *redisCluster) CloseAll() {
	for _, v := range r.configNodes {
		v.Client.Close()
	}
	r.conn.Close()
}

func Fresh(client *Client, nodeList map[string]*RedisNodes) (err error) {
	val, err := client.ClusterNodes()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	for _, nodeInfo := range strings.Split(val, "\n") {
		info := strings.Split(nodeInfo, " ")
		if len(info) < 8 { //invaliad cluster node info
			continue
		}
		nodeType := getIndex(info[2], HostType[:2], strings.Contains)
		if nodeType == -1 {
			log.Fatal(fmt.Sprintf("unknown type %s", info[2]))
		}
		nodeStatus := getIndex(info[7], ConnStatus[:2], strings.Contains)
		if nodeStatus == -1 {
			log.Fatal(fmt.Sprintf("unknown Status %s", info[7]))
		}
		if len(info) == 8 { //the node info don't have any slot
			nodeList[info[1]] = &RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus}
		} else { // the node has slot
			slotList := make([][]int, 0)
			for _, slot := range info[8:] {
				if strings.Contains(slot, "-") { //it has an range slot
					var from, to int
					slotInfo := strings.Split(slot, "-")
					from, err = strconv.Atoi(slotInfo[0])
					if err != nil {
						return
					}
					to, err = strconv.Atoi(slotInfo[1])
					if err != nil {
						return
					}
					slotList = append(slotList, []int{from, to})
				} else { // it has only one slot
					var slotNum int
					slotNum, err = strconv.Atoi(slot)
					if err != nil {
						return
					}
					slotList = append(slotList, []int{slotNum})
				}
			}
			nodeList[info[1]] = &RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus, Slot: slotList}
		}

	}

	getNodeById := func(Id string) (rv *RedisNodes) {
		for _, node := range nodeList {
			if node.Id == Id {
				rv = node
				return
			}
		}

		rv = nil
		return
	}

	for _, node := range nodeList {
		if node.Type == Slave {
			node.Master = getNodeById(node.Master).IP
		}
	}
	return

}

func FreshThroughHost(host string, node map[string]*RedisNodes) {
	conn, err := Dial(host)
	if err != nil {
		log.Fatal("faild to connect %s\n", host)
	}

	Fresh(conn, node)
}

// get the new cluster info
func (r *redisCluster) FreshClusterInfo() (err error) {
	r.ShowConfig()
	for _, node := range r.configNodes {
		err = Fresh(node.Connect(), r.runNodes)
		if err == nil {
			return
		}
	}
	log.Fatal(err.Error())

	return
}

// change the host replicate from the master
func (r *redisCluster) replicate(host string, master *RedisNodes) (err error) {
	conn := r.configNodes[host].Connect()
	err = conn.ClusterReplicate(master.Id)
	return
}

func (r *redisCluster) Replicate(host string, master string) {
	if !IsValidIP(host) || !IsValidIP(master) {
		log.Fatal("invalId host format!")
	}
	node, has := r.runNodes[master]
	if has {
		if err := r.replicate(host, node); err != nil {
			log.Fatal(err.Error())
		}
	} else {
		log.Fatal(fmt.Sprintf("can't found the Client %s on the node list", node))
	}
}

// do failover on the salve node, so it can replace the its master node
func (r *redisCluster) FailOver(slavehost string) (err error) {
	conn := r.configNodes[slavehost].Connect()
	err = conn.ClusterFailover()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

// Delete an slot from node
/*func (r *redisCluster) DelSlot(host string, min, max int) (err error) {
	conn := r.configNodes[host].Connect()
	_, err = conn.ClusterDelSlotsRange(min, max).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}
*/
// count how many key in the slot
func (r *redisCluster) CountKeys(host string, slot int) (num int, err error) {
	conn := r.configNodes[host].Connect()
	if num, err = conn.ClusterCountKeysInSlot(slot); err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

type StringSlice []string

func (s StringSlice) Len() (rv int) {
	rv = len(s)
	return
}

func (s StringSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s StringSlice) Less(i, j int) (rv bool) {
	var num1, num2 int

	if strings.Contains(s[i], "-") {
		num := strings.Split(s[i], "-")
		num1, _ = strconv.Atoi(num[0])
	} else {
		num1, _ = strconv.Atoi(s[i])
	}
	if strings.Contains(s[j], "-") {
		num := strings.Split(s[j], "-")
		num2, _ = strconv.Atoi(num[0])
	} else {
		num2, _ = strconv.Atoi(s[j])
	}
	rv = num1 < num2
	return
}

// Take a snap shot
func (r *redisCluster) TakeSnapShot(filename string) (err error) {
	nodeList := make([]RedisNodes, 0)
	for _, node := range r.runNodes {
		nodeList = append(nodeList, *node)
	}

	content, err := json.MarshalIndent(redisCluster{Schema: nodeList, Logfile: r.Logfile}, "", "\t")
	if err != nil {
		log.Fatal(err.Error())
	}
	if filename != "" {
		fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer fd.Close()
		fd.Write(content)
		fd.WriteString("\n")
	} else {
		fmt.Println(string(content))
	}
	return
}

/*
func (r *redisCluster) sortSlot() {
	slots := make(StringSlice, 0, 10)
	for _, node := range r.configNodes {
		if node.Master == "slave" || len(node.Slot) == 0 {
			continue
		}
		slots = append(slots, node.Slot...)
	}
	fmt.Println(slots)
	sort.Sort(slots)
	fmt.Println(slots)
}


func MergeSlot(slots []string) {
	for i := 0; i < len(slots); i++ {
		if strings.Contains(slots[i], "-") {
			current := strings.Split(slots[i], "-")
		}
	}
}
*/
/*func (r *redisCluster) hasSlot(slot int) {
	find := false
	for _, node := range r.configNodes {
		if node.Master == "Slave" || len(node.Slot) == 0 {
			continue
		}
		for _, slots := range node.Slot {
			if strings.Contains("-") {
				number = strings.Split(num)
			}
		}
	}
}

func (r *redisCluster) CheckSlot() (slot []string) {
	for i := 0; i < 16000; i++ {

	}
}
*/
