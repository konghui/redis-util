package main

// check the IP is valid or not
func IsValidIP(host string) (rv bool) {
	rv = true
	return
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
