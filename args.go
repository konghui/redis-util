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
	app.Name = "redis-utils"
	app.Usage = "Take a snapshot or restore a cluster from an snapshot"
	app.UsageText = fmt.Sprintf("%s [global options] command [command options]", app.Name)
	app.Commands = []cli.Command{
		restoreCommand,
		saveCommand,
		deleteCommand,
		addCommand,
	}
	app.Run(os.Args)
}
