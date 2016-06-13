package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func InitArgs() {
	app := cli.NewApp()
	app.Email = "konghui@youku.com"
	app.Author = "konghui"
	app.Version = "1.0"
	app.Name = "redis_napshot"
	app.Usage = "Take a snapshot or restore a cluster from an snapshot"
	app.UsageText = fmt.Sprintf("%s [global options] command [command options]", app.Name)
	app.Action = func(c *cli.Context) error {
		fmt.Println("Hello", c.Args()[0])
		return nil
	}

	app.Action = func(c *cli.Context) error {
		return nil
	}
	app.Commands = []cli.Command{
		{
			Name:        "restore",
			Aliases:     []string{"-r"},
			Description: fmt.Sprintf("restore the redis cluster status from json config\n   for example:\n        %s restore config.json", app.Name),
			Usage:       "config.json",
			ArgsUsage:   "FILE.",
			Action: func(c *cli.Context) (err error) {
				if len(c.Args()) < 1 {
					fmt.Println("Json file must be provide")
					return
				}
				myargs := make(map[string]string)
				myargs["file"] = c.Args().First()
				rc := RedisSnapShot(myargs)
				defer rc.CloseAll()
				err = rc.Restore()

				return

			},
		},
		{
			Name:        "save",
			Aliases:     []string{"-s"},
			Description: fmt.Sprintf("save the redis cluster infomation to an json file or show in the stdin\n   for example:\n        %s save -f test.json \tconnect through the json file\n        %s save 127.0.0.1:7000 \tconnect through the node\n        %s save 127.0.0.1:7000 --file save.json \tsave to an json file,other wise show in the stdin", app.Name, app.Name, app.Name),
			ArgsUsage:   "ip:port.",
			Usage:       "snapshot.json",
			Action: func(c *cli.Context) (err error) {
				if len(c.Args()) < 1 {
					fmt.Println("ip:port must be provide")
					return
				}
				myargs := make(map[string]string)
				myargs["host"] = c.Args().First()
				rc := RedisSnapShot(myargs)
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
		},
		{
			Name:        "delete",
			Aliases:     []string{"-d"},
			Description: fmt.Sprintf("delete a node from the redis cluster\n    for example:\n        %s delete 127.0.0.1:7000 127.0.0.1:7001\n        %s delete 127.0.0.1:7000 --file config.json\n        127.0.0.1:7000 --host 127.0.0.1:7002", app.Name, app.Name, app.Name),
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
				rc := RedisSnapShot(myargs)
				defer rc.CloseAll()
				err = rc.ForgetAll(rc.runNodes[c.Args().First()])
				return
			},
		},
	}
	app.Run(os.Args)

}
