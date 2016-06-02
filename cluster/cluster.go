package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/konghui/redis_snapshot/common"
	"github.com/konghui/redis_snapshot/nodes"
	"gopkg.in/redis.v3"
)

const (
	MAX_RETRY      = 10
	RETRY_INTERVAL = 5
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
	Schema      []nodes.RedisNodes
	logger      *log.Logger
	Logfile     string
	configNodes map[string]*nodes.RedisNodes
	conn        *redis.ClusterClient
	runNodes    map[string]*nodes.RedisNodes
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

	fd, err := os.OpenFile(r.Logfile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err.Error())
		os.Exit(1)
	}
	r.logger = log.New(fd, "redis_snapshot", log.Ldate|log.Ltime|log.Lshortfile)
}

func (r *redisCluster) ParserFromHost(host string) {
	FreshThroughHost(host, r.configNodes)
}

func RedisSnapShot(args map[string]string) (r *redisCluster) {
	var snapShot redisCluster
	snapShot.configNodes = make(map[string]*nodes.RedisNodes)
	snapShot.runNodes = make(map[string]*nodes.RedisNodes)
	if val, has := args["file"]; has {
		snapShot.ParseFromFile(val)
	} else if val, has := args["host"]; has {
		snapShot.ParserFromHost(val)
	}
	snapShot.initNodes()

	return &snapShot
}

// copy configure to the nodes
func (r *redisCluster) registerNodes(node *nodes.RedisNodes) {
	r.configNodes[node.IP] = node
}

func (r *redisCluster) initNodes() {
	for i := 0; i < len(r.Schema); i++ {
		r.configNodes[r.Schema[i].IP] = &r.Schema[i]

	}
	r.ConnectAll()
	r.FreshClusterInfo()
}

func nodeStatus(nodes map[string]*nodes.RedisNodes) {
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
	i := 0
	for _, v := range r.configNodes {
		v.Connect()
		list[i] = v.IP
		i++
	}
	fmt.Println(list)
	r.conn = redis.NewClusterClient(&redis.ClusterOptions{Addrs: list})
	if r.conn == nil {
		log.Fatal("faild to connect %s\n", list)
	}
}

func getIndex(str string, list []string, compare func(string, string) bool) (rv int) {
	for index, value := range list {
		if compare(str, value) {
			rv = index
			return
		}
	}
	rv = -1
	return
}

func callbackNodeInList(node *nodes.RedisNodes, args interface{}) bool {
	if node.Type == Master {
		return true
	}
	val, err := node.Client.ClusterNodes().Result()
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
func (r *redisCluster) CheckAllNode(callback func(*nodes.RedisNodes, interface{}) bool, args interface{}) bool {
	for _, redisNode := range r.configNodes {
		if !callback(redisNode, args) {
			return false
		}
	}
	return true
}

func Fresh(client *redis.ClusterClient, nodeList map[string]*nodes.RedisNodes) (err error) {
	val, err := client.ClusterNodes().Result()
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
			nodeList[info[1]] = &nodes.RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus}
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
			nodeList[info[1]] = &nodes.RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus, Slot: slotList}
		}

	}

	getNodeById := func(Id string) (rv *nodes.RedisNodes) {
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

func FreshThroughHost(host string, node map[string]*nodes.RedisNodes) {
	conn := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{host}})
	if conn == nil {
		log.Fatal("faild to connect %s\n", host)
	}
	Fresh(conn, node)
}

// get the new cluster info
func (r *redisCluster) FreshClusterInfo() (err error) {
	err = Fresh(r.conn, r.runNodes)
	/*	val, err := r.conn.ClusterNodes().Result()
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
				r.runNodes[info[1]] = &nodes.RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus}
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
				r.runNodes[info[1]] = &nodes.RedisNodes{Id: info[0], IP: info[1], Type: nodeType, Master: info[3], Status: nodeStatus, Slot: slotList}
			}

		}

		getNodeById := func(Id string) (rv *nodes.RedisNodes) {
			for _, node := range r.runNodes {
				if node.Id == Id {
					rv = node
					return
				}
			}

			rv = nil
			return
		}

		for _, node := range r.runNodes {
			if node.Type == Slave {
				node.Master = getNodeById(node.Master).IP
			}
		}*/
	return
}

// change the host replicate from the master
func (r *redisCluster) replicate(host string, master *nodes.RedisNodes) (err error) {
	conn := r.configNodes[host].Connect()
	_, err = conn.ClusterReplicate(master.Id).Result()
	return
}

func (r *redisCluster) Replicate(host string, master string) {
	if !common.IsValidIP(host) || !common.IsValidIP(master) {
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

// add a master node
// host string: ip:port => 127.0.0.1:8000
func (r *redisCluster) AddHost(host string, nodeType int) (err error) {
	if !common.IsValidIP(host) || nodeType == -1 {
		log.Fatal("invalId host format or node type!")
	}
	hostInfo := strings.Split(host, ":")
	_, err = r.conn.ClusterMeet(hostInfo[0], hostInfo[1]).Result()
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
			if !common.IsValidIP(node.Master) { // check the master ip is valId, split with ":" such as "127.0.0.1:8000"
				err = errors.New(fmt.Sprintf("invalId Master config of slave %s", host))
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
				if r.runNodes[node.Master].Type == Slave { //if the master node was configure as the slave node or not
					err = errors.New(fmt.Sprintf("Node %s is a slave node, can't change it to the master state", node.Master))
					log.Fatal(err.Error())
					return
				}
			} else { // if the master node not found on the json file
				fmt.Printf("Master node %s not exist!\n", node.IP)
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
	return
}

func (r *redisCluster) forget(host string, Id string) (err error) {
	conn := r.configNodes[host].Connect()
	_, err = conn.ClusterForget(Id).Result()
	return
}

// do forget on the all the node, queal delete the node from the cluster
func (r *redisCluster) ForgetAll(forgetHost *nodes.RedisNodes) (err error) {
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

// do failover on the salve node, so it can replace the its master node
func (r *redisCluster) FailOver(slavehost string) (err error) {
	conn := r.configNodes[slavehost].Connect()
	_, err = conn.ClusterFailover().Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
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

func (r *redisCluster) SetNode(nodeInfo *nodes.RedisNodes) (err error) {
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

func (r *redisCluster) SetNodeSlot(nodeinfo *nodes.RedisNodes) {
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

func (r *redisCluster) getNodeBySlot(slot int, max int) (find *nodes.RedisNodes, end int) {
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

func getSlotInfo(slot []int) (from, to int) {
	length := len(slot)
	if length == 2 { // if the slot is like 1-2
		from = slot[0]
		to = slot[1]
	} else if length == 1 { // only one slot like 1024
		from = slot[0]
		to = slot[0]
	} else {
		log.Fatal("unknown slot %s", slot)
	}
	return
}

func (r *redisCluster) SetNodeType(nodeInfo *nodes.RedisNodes) (err error) {
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

func (r *redisCluster) AddSlot(slot int) (err error) {
	if _, err = r.conn.ClusterAddSlots(slot).Result(); err != nil {
		log.Fatal(err.Error())
	}
	return
}

func (r *redisCluster) SetSlot(host string, slot int, cmd, nodeId string) (err error) {
	log.Printf("IP=>%s, slot=>%d, cmd=%s, Id=%s\n", host, slot, cmd, nodeId)
	if r.runNodes[host].Type == Slave {
		return
	}
	conn := r.configNodes[host].Connect()
	_, err = conn.ClusterSetSlot(slot, cmd, nodeId).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

// Delete an slot from node
func (r *redisCluster) DelSlot(host string, min, max int) (err error) {
	conn := r.configNodes[host].Connect()
	_, err = conn.ClusterDelSlotsRange(min, max).Result()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

// count how many key in the slot
func (r *redisCluster) CountKeys(host string, slot int) (num int64, err error) {
	conn := r.configNodes[host].Connect()
	if num, err = conn.ClusterCountKeysInSlot(slot).Result(); err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

func (r *redisCluster) MoveSlot(fromHost, toHost *nodes.RedisNodes, slot int) (err error) {
	if fromHost.IP == toHost.IP { //slot in the correct node
		return
	}
	log.Printf("move slot %d from %s to %s\n", slot, fromHost.IP, toHost.IP)
	r.SetSlot(fromHost.IP, slot, "migrating", toHost.Id) // set  src node to the migrating state
	r.SetSlot(toHost.IP, slot, "importing", fromHost.Id) // set dst node to the importing state
	keyNum, err := r.CountKeys(fromHost.IP, slot)        // get the total number
	for _, key := range r.GetKeysInSlot(fromHost.IP, slot, int(keyNum)) {
		r.Migrate(fromHost.IP, toHost.IP, key, 0, 5000)
	}

	r.SetSlot(fromHost.IP, slot, "node", toHost.Id)
	r.SetSlot(toHost.IP, slot, "node", toHost.Id)
	r.SetAllSlot(slot, toHost)
	return
}

func (r *redisCluster) SetAllSlot(slot int, toHost *nodes.RedisNodes) {
	for _, node := range r.runNodes {
		if node.Type == Master {
			r.SetSlot(node.IP, slot, "node", toHost.Id)
		}
	}
}

func (r *redisCluster) DelAllSlot(min, max int) {
	for _, node := range r.runNodes {
		r.DelSlot(node.IP, min, max)
	}
}

func (r *redisCluster) Migrate(from, to, key string, db int64, timeout time.Duration) (err error) {
	conn := r.configNodes[from].Connect()
	toHost := strings.Split(to, ":")
	if _, err = conn.Migrate(toHost[0], toHost[1], key, db, timeout).Result(); err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

func (r *redisCluster) GetKeysInSlot(host string, slot, num int) (keyList []string) {
	conn := r.configNodes[host].Connect()
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

// Take a snap shot
func (r *redisCluster) TakeSnapShot(filename string) (err error) {
	nodeList := make([]nodes.RedisNodes, 0)
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
