package main

import (
	"flag"
	"fmt"

	"github.com/ilyq69/go-stash-es/stash/config"
	"github.com/ilyq69/go-stash-es/stash/es"
	"github.com/ilyq69/go-stash-es/stash/output"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/config.yaml", "Specify the config file")

func main() {
	var c config.Config
	conf.MustLoad(*configFile, &c)

	var handler es.ConsumeHandler
	var err error
	if c.Output.Target == "csv" {
		handler, err = output.NewCSVHandler(c.Output.Filename, c.ESConf.Source)
	} else if c.Output.Target == "json" {
		handler, err = output.NewJsonFileHandler(c.Output.Filename)
	} else {
		panic(fmt.Sprintf("%s not support!", c.Output.Target))
	}
	logx.Must(err)

	es, err := es.NewES(c.ESConf, handler)
	logx.Must(err)

	es.Start()
	es.Close()
}
