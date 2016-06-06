package main

import (
	"log"
	"strings"
)

func (r *redisCluster) AddSlot(slot int) (err error) {
	if err = r.conn.ClusterAddSlots(slot); err != nil {
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
	err = conn.ClusterSetSlot(slot, cmd, nodeId)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

func (r *redisCluster) MoveSlot(fromHost, toHost *RedisNodes, slot int) (err error) {
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

func (r *redisCluster) SetAllSlot(slot int, toHost *RedisNodes) {
	for _, node := range r.runNodes {
		if node.Type == Master {
			r.SetSlot(node.IP, slot, "node", toHost.Id)
		}
	}
}

/*func (r *redisCluster) DelAllSlot(min, max int) {
	for _, node := range r.runNodes {
		r.DelSlot(node.IP, min, max)
	}
}*/

func (r *redisCluster) Migrate(from, to, key string, db int, timeout int) (err error) {
	conn := r.configNodes[from].Connect()
	toHost := strings.Split(to, ":")
	if err = conn.Migrate(toHost[0], toHost[1], key, db, timeout); err != nil {
		log.Fatal(err.Error())
		return
	}
	return
}

func (r *redisCluster) GetKeysInSlot(host string, slot, num int) (keyList []string) {
	conn := r.configNodes[host].Connect()
	keyList, err := conn.ClusterGetKeysInSlot(slot, num)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	log.Printf("find key %s in slot %d\n", keyList, slot)
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
