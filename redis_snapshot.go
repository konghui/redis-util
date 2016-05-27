package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v3"
)

type redisNodes struct {
	IP     string
	Type   string
	Master string
	Slot   []string
	status string
	id     string
	client *redis.Client
}

type redisCluster struct {
	Schema      []redisNodes
	logger      *log.Logger
	Logfile     string
	configNodes map[string]*redisNodes
	conn        *redis.ClusterClient
	runNodes    map[string]*redisNodes
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

func RedisFixer(configfile string) (r *redisCluster) {
	var fixer redisCluster
	fixer.configNodes = make(map[string]*redisNodes)
	fixer.runNodes = make(map[string]*redisNodes)
	if err := fixer.ConfigParser(configfile); err != nil {
		log.Fatal(err.Error())
	}

	fd, err := os.OpenFile(fixer.Logfile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err.Error())
		os.Exit(1)
	}
	fixer.logger = log.New(fd, "redis_fix", log.Ldate|log.Ltime|log.Lshortfile)
	fixer.initNodes()
	fixer.ConnectAll()
	fixer.FreshClusterInfo()
	return &fixer
}

// copy configure to the nodes
func (r *redisCluster) registerNodes(node *redisNodes) {
	r.configNodes[node.IP] = node
}

func (r *redisCluster) initNodes() {
	for i := 0; i < len(r.Schema); i++ {
		r.configNodes[r.Schema[i].IP] = &r.Schema[i]
	}
}

func (r *redisCluster) printConfig() {
	fmt.Printf("logfile=>%s\n", r.Logfile)
	for _, v := range r.configNodes {
		fmt.Printf("IP=>%s\tMaster=>%s\tType=%s\tSlot=%s\tid=%s\tstatus=%s\n", v.IP, v.Master, v.Type, v.Slot, v.id, v.status)
	}
	fmt.Printf("running node info:\n")
	for _, v := range r.runNodes {
		fmt.Printf("IP=>%s\tMaster=>%s\tType=%s\tSlot=%s\tid=%s\tstatus=%s\n", v.IP, v.Master, v.Type, v.Slot, v.id, v.status)
	}

}

// connect to the all server in the list
func (r *redisCluster) ConnectAll() {
	list := make([]string, len(r.configNodes))
	i := 0
	for _, v := range r.configNodes {
		r.Connect(v.IP)
		list[i] = v.IP
		i++
	}
	r.conn = redis.NewClusterClient(&redis.ClusterOptions{Addrs: list})
	if r.conn == nil {
		log.Fatal("faild to connect %s\n", list)
	}
}

// get the connect from the runNode
func (r *redisCluster) Connect(host string) (conn *redis.Client) {
	if node, has := r.runNodes[host]; has {
		if node.client == nil { // has no redisClient is nil, try to connect
			node.client = redis.NewClient(&redis.Options{Addr: node.IP})
		}
		conn = node.client
	}
	return
}

func (r *redisCluster) FreshClusterInfo() (err error) {
	val, err := r.conn.ClusterNodes().Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	for _, nodeinfo := range strings.Split(val, "\n") {
		info := strings.Split(nodeinfo, " ")
		if len(info) < 8 { //invaliad cluster node info
			continue
		} else if len(info) == 8 { //the node info don't have any slot
			r.runNodes[info[1]] = &redisNodes{id: info[0], IP: info[1], Type: info[2], Master: info[3], status: info[7]}
		} else { // the node has slot
			r.runNodes[info[1]] = &redisNodes{id: info[0], IP: info[1], Type: info[2], Master: info[3], status: info[7], Slot: info[8:]}
		}

	}
	return
}

func (r *redisCluster) Replicate(host string, master string) (err error) {
	if !strings.Contains(host, ":") || !strings.Contains(master, ":") {
		err = errors.New("invalid host format!")
		os.Exit(1)
	}
	conn := r.Connect(host)
	node, has := r.runNodes[master]
	if has {
		conn.ClusterReplicate(node.id)
	} else {
		err = errors.New(fmt.Sprintf("can't found the client %s on the node list", node))
	}
	return
}

// add a master node
// host string: ip:port => 127.0.0.1:8000
func (r *redisCluster) AddHost(host string, nodetype string) (err error) {
	if !strings.Contains(host, ":") {
		err = errors.New("invalid host format!")
		os.Exit(1)
	}
	hostInfo := strings.Split(host, ":")
	val, err := r.conn.ClusterMeet(hostInfo[0], hostInfo[1]).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	r.FreshClusterInfo()
	if nodetype == "slave" { // if it was a slave node
		node, has := r.configNodes[host]
		if has { //if this node has configuration on the configuration file
			if !strings.Contains(node.Master, ":") { // check the master ip is valiad, split with ":" such as "127.0.0.1:8000"
				err = errors.New(fmt.Sprintf("invalid Master config of slave %s", host))
				log.Fatal(err.Error())
				return
			}

			node, has = r.configNodes[node.Master]
			if has { //if the slave's master has configuration on the configuration file
				_, has = r.runNodes[node.Master] // check the master node exists on the online environment or not
				if !has {                        // if not exists add it
					if err = r.AddHost(node.IP, node.Type); err != nil {
						log.Fatal(err.Error())
						return
					}
				}
				if r.runNodes[node.Master].Type == "slave" { //if the master node was configure as the slave node or not
					err = errors.New(fmt.Sprintf("Node %s is a slave node, can't change it to the master state", node.Master))
					log.Fatal(err.Error())
					return
				}
			} else { // if the master node not found on the json file
				fmt.Printf("Master node %s not exist!\n", node.IP)
				os.Exit(1)
			}
		}
		if err = r.Replicate(node.IP, node.Master); err != nil { //change the node to the slave status
			log.Fatal(err.Error())
			return
		}
		r.FreshClusterInfo() //refreash the local node list

	}
	fmt.Printf("val=%s\n", val)
	return
}

// delete the node from cluster
func (r *redisCluster) Forget(host string, forgetHost string) (err error) {
	if !strings.Contains(host, ":") || !strings.Contains(forgetHost, ":") {
		err = errors.New("invalid host format!")
		return
	}
	node, has := r.runNodes[forgetHost]
	if !has {
		err = errors.New("id should not be empty!")
		log.Fatal(err.Error())
		return
	}
	conn := r.Connect(host)
	val, err := conn.ClusterForget(node.id).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	fmt.Printf("val=%s\n", val)
	return
}

// do forget on the all the node, queal delete the node from the cluster
func (r *redisCluster) ForgetAll(forgetHost string) (err error) {
	for host := range r.runNodes {
		r.Forget(host, forgetHost)
	}
	r.FreshClusterInfo()
	return
}

// do failover on the salve node, so it can replace the its master node
func (r *redisCluster) FailOver(slavehost string) (err error) {
	conn := r.Connect(slavehost)
	_, err = conn.ClusterFailover().Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

// fix the cluster
func (r *redisCluster) Fix() (err error) {
	fmt.Println("Try to check and fix the cloud........")
	r.SetAllNodeType()
	r.SetAllNodeSlot()
	return
}

func (r *redisCluster) RemoveDisconnectNode() {
	for _, node := range r.runNodes {
		if node.status == "disconnect" {
			r.ForgetAll(node.IP)
		}
	}
}

func (r *redisCluster) SetAllNodeType() (err error) {
	for _, node := range r.configNodes {
		if err = r.SetNodeType(node); err != nil {
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

func (r *redisCluster) SetNode(nodeinfo *redisNodes) (err error) {
	node, has := r.runNodes[nodeinfo.IP]
	if !has {
		fmt.Printf("node %s not exists! Add it\n", node.IP)
		if err = r.AddHost(node.IP, node.Type); err != nil {
			return
		}
	}
	if err = r.SetNodeType(nodeinfo); err != nil {
		return
	}
	r.SetNodeSlot(nodeinfo)
	return
}

func (r *redisCluster) SetNodeSlot(nodeinfo *redisNodes) {
	if len(nodeinfo.Slot) == 0 {
		return
	}
	runinfo := r.runNodes[nodeinfo.IP]
	for _, slotDst := range nodeinfo.Slot {
		fromDst, toDst := getSlotInfo(slotDst)
		for i := fromDst; i <= toDst; {
			fmt.Printf("toDst = %d\n", toDst)
			nodeSrc, end := r.getNodeBySlot(i, toDst)
			if nodeSrc == nil {
				r.AddSlot(i)
				i++
				continue
			}
			for j := i; j <= end; j++ {
				//fmt.Printf("move slot from %s to %s slot %d\n", nodeinfo.IP, nodeSrc.IP, j)
				r.MoveSlot(nodeSrc, runinfo, j)
			}
			i = end + 1
		}
	}
	r.FreshClusterInfo()
}

func (r *redisCluster) getNodeBySlot(slot int, max int) (find *redisNodes, end int) {
	for _, node := range r.runNodes { // if it's a slave node and has no slot skip it
		if node.Type == "slave" || len(node.Slot) == 0 {
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

func getSlotInfo(slot string) (from, to int) {
	if strings.Contains(slot, "-") { // if the slot is like 1-2
		slotInfo := strings.Split(slot, "-")
		from, _ = strconv.Atoi(slotInfo[0])
		to, _ = strconv.Atoi(slotInfo[1])
	} else { // only one slot like 1024
		from, _ = strconv.Atoi(slot)
		to, _ = strconv.Atoi(slot)
	}
	return
}

func (r *redisCluster) SetNodeType(nodeinfo *redisNodes) (err error) {
	if strings.Contains(r.runNodes[nodeinfo.IP].Type, nodeinfo.Type) {
		fmt.Printf("node %s is in the correct state config.type=%s  run.type=%s\n", nodeinfo.IP, r.runNodes[nodeinfo.IP].Type, nodeinfo.Type)
		return
	}
	fmt.Printf("node %s is in an incorrect state, try to fix it\n", nodeinfo.IP)
	if nodeinfo.Type == "master" {
		if err = r.FailOver(nodeinfo.IP); err != nil {
			return
		}
	} else if nodeinfo.Type == "slave" {
		if err = r.Replicate(nodeinfo.IP, nodeinfo.Master); err != nil {
			return
		}
	} else {
		err = errors.New(fmt.Sprintf("unknown type %s", nodeinfo.Type))
	}
	// compare slot here
	return
}

func getMasterNodes(nodes map[string]*redisNodes) (master []string) {
	return
}

func (r *redisCluster) AddSlot(slot int) (err error) {
	if _, err = r.conn.ClusterAddSlots(slot).Result(); err != nil {
		log.Fatal(err.Error())
	}
	return
}

func (r *redisCluster) SetSlot(host string, slot int, cmd, nodeId string) (err error) {
	fmt.Printf("IP=>%s, slot=>%d, cmd=%s, id=%s\n", host, slot, cmd, nodeId)
	conn := r.Connect(host)
	val, err := conn.ClusterSetSlot(slot, cmd, nodeId).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	fmt.Println(val)
	return
}

func (r *redisCluster) DelSlot(host string, min, max int) (err error) {
	conn := r.Connect(host)
	val, err := conn.ClusterDelSlotsRange(min, max).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	fmt.Println(val)
	return
}

func (r *redisCluster) CountKeys(host string, slot int) (num int64, err error) {
	conn := r.Connect(host)
	if num, err = conn.ClusterCountKeysInSlot(slot).Result(); err != nil {
		fmt.Println(err.Error())
		return
	}
	return
}

func (r *redisCluster) MoveSlot(fromHost, toHost *redisNodes, slot int) (err error) {
	if fromHost.IP == toHost.IP {
		//fmt.Printf("skip slot %d\n", slot)
		return
	}
	log.Printf("move slot %d from %s to %s\n", slot, fromHost.IP, toHost.IP)
	r.FreshClusterInfo()
	//	return
	r.printConfig()
	r.SetSlot(fromHost.IP, slot, "migrating", toHost.id)
	r.SetSlot(toHost.IP, slot, "importing", fromHost.id)
	keyNum, err := r.CountKeys(fromHost.IP, slot) // get the total number
	for _, key := range r.GetKeysInSlot(fromHost.IP, slot, int(keyNum)) {
		r.Migrate(fromHost.IP, toHost.IP, key, 0, 5000)
	}
	//r.SetSlot(fromHost.IP, slot, "stable", "")
	//r.SetSlot(toHost.IP, slot, "stable", "")
	//val, err := r.conn.ClusterSetSlot(slot, "node", toHost.id).Result()
	//if err != nil {
	//	log.Fatal(err.Error())
	//}
	//fmt.Println(val)
	r.SetSlot(fromHost.IP, slot, "node", toHost.id)
	r.SetSlot(toHost.IP, slot, "node", toHost.id)
	r.SetAllSlot(slot, toHost)
	return
}

func (r *redisCluster) SetAllSlot(slot int, toHost *redisNodes) {
	for _, node := range r.runNodes {
		if strings.Contains(node.Type, "master") {

			r.SetSlot(node.IP, slot, "node", toHost.id)
		}
	}
}

func (r *redisCluster) DelAllSlot(min, max int) {
	for _, node := range r.runNodes {
		r.DelSlot(node.IP, min, max)
	}
}

func (r *redisCluster) Migrate(from, to, key string, db int64, timeout time.Duration) (err error) {
	conn := r.Connect(from)
	toHost := strings.Split(to, ":")
	if _, err = conn.Migrate(toHost[0], toHost[1], key, db, timeout).Result(); err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

func (r *redisCluster) GetKeysInSlot(host string, slot, num int) (keyList []string) {
	conn := r.Connect(host)
	keyList, err := conn.ClusterGetKeysInSlot(slot, num).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	log.Printf("find key %s in slot %d\n", keyList, slot)
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

/*
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
		if node.Master == "slave" || len(node.Slot) == 0 {
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
func main() {
	cluster := RedisFixer("config.json")
	//cluster.printConfig()
	//	if err := fixer.AddMaster("127.0.0.1:7008"); err != nil {
	//		fmt.Println(err.Error())
	//	}
	//if err := fixer.Forget("127.0.0.1:7008"); err != nil {
	//	fmt.Println(err.Error())
	//}
	//fixer.ForgetAll("127.0.0.1:7008")
	//if err := fixer.FailOver("127.0.0.1:7000"); err != nil {
	//	fmt.Println(err.Error())
	//}
	//	fixer.FreshClusterInfo()
	//	fixer.printConfig()
	cluster.Fix()
	//if err := cluster.SetSlot("127.0.0.1:7000", 11, "node", "aa81cfa967c26b7a2cf8f2388044a38d4cc448f5"); err != nil {
	//	fmt.Println(err.Error())
	//}
	//if err := cluster.Migrate("127.0.0.1:7000", "127.0.0.1:7006", "foo847", 0, 2000000); err != nil {
	//	fmt.Println(err.Error())
	//}
	//cluster.GetKeysInSlot("127.0.0.1:7000", 3, 10)
	//if num, err := cluster.CountKeys("127.0.0.1:7000", 3); err != nil {
	//	fmt.Println(err.Error())
	//} else {
	//	fmt.Printf("num=%d\n", num)
	//}
	//from := cluster.runNodes["127.0.0.1:7007"]
	//to := cluster.runNodes["127.0.0.1:7001"]
	//cluster.MoveSlot(to, from, 10923)
	//cluster.sortSlot()
}
