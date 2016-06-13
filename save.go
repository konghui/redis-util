package main

import (
	"fmt"

	"os"

	"github.com/urfave/cli"
)

var saveCommand = cli.Command{
	Name:        "save",
	Aliases:     []string{"-s"},
	Description: fmt.Sprintf("save the redis cluster infomation to an json file or show in the stdin\n   for example:\n        %s save -f test.json \tconnect through the json file\n        %s save 127.0.0.1:7000 \tconnect through the node\n        %s save 127.0.0.1:7000 --file save.json \tsave to an json file,other wise show in the stdin", os.Args[0], os.Args[0], os.Args[0]),
	ArgsUsage:   "ip:port.",
	Usage:       "snapshot.json",
	Action: func(c *cli.Context) (err error) {
		if len(c.Args()) < 1 {
			fmt.Println("ip:port must be provide")
			return
		}
		myargs := make(map[string]string)
		myargs["host"] = c.Args().First()
		rc := RedisTrib(myargs)
		defer rc.CloseAll()
		err = rc.TakeSnapShot(c.String("file"))
		return nil
	},
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "file",
			Value: "",
			Usage: "snapshot.json",
		},
	},
}
