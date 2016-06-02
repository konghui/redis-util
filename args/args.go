package args

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func hello(c *cli.Context) error {
	fmt.Println(c.Args().First())
	fmt.Print(c.Args())
	return nil
}

func hello1(c *cli.Context) error {
	fmt.Println("restore")
	return nil
}

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
	/*app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "restore",
			Value: "snapshot.json",
			Usage: "restore the redis cluster status from json config",
		},
		cli.StringFlag{
			Name:  "save",
			Value: "export.json",
			Usage: "export the redis cluster infomation to an json file",
		},
		cli.StringFlag{
			Name:  "add",
			Usage: "add an new node to the cluster",
		},
	}*/
	app.Commands = []cli.Command{
		{
			Name:        "restore",
			Aliases:     []string{"-r"},
			Description: fmt.Sprintf("restore the redis cluster status from json config\n    for example:\n\t%s restore config.json", app.Name),
			Usage:       "config.json",
			Action:      hello1,
		},
		{
			Name:        "save",
			Aliases:     []string{"-s"},
			Description: fmt.Sprintf("save the redis cluster infomation to an json file\n   for example:\n\t%s save -f test.json\n%s save 127.0.0.1:7000", app.Name),
			Usage:       "snapshot.json",
		},
		{
			Name:        "delete",
			Aliases:     []string{"-d"},
			Description: fmt.Sprintf("delete a node from the redis cluster\n    for example:\n\t%s delete 127.0.0.1:8000", app.Name),
			Usage:       "ip:port",
			Action:      hello,
		},
	}
	app.Run(os.Args)

}
