package main

// main - entry point of the metrics analyzer

import (
	"fmt"
	"log"
	"os"

	"../analysis"
	"../config"
	"../ingress"
	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Database        string `short:"n" long:"database" description:"Database name" default:"sandbox"`
	CreateDB        bool   `short:"c" long:"create" description:"Create database"`
	DropDB          bool   `short:"d" long:"drop" description:"Drop database"`
	InputFile       string `short:"f" long:"file" description:"Input file containing the MP queries" default:""`
	Query           string `short:"q" long:"query" description:"Query to execute:{elapsed|elapsed_rate|cpu|cpu_rate|success_rate|ssq_elapsed_host|ssq_elapsed_pool}" default:""`
	Days            int    `short:"t" long:"days" description:"Days to compute the rates" default:"1"`
	Env             string `short:"e" long:"env" description:"Environment:{onebox|dev|prod}" default:"onebox"`
	Mutations       int    `short:"m" long:"mutations" description:"Create M different mutations with the same query signature" default:"0"`
	Percentile      int    `short:"p" long:"percentile" description:"Percentile 0-100 for slowest subquery metrics" default:"50"`
	RetentionPolicy string `short:"r" long:"retention-name" description:"Retention policy name" default:""`
	RetentionTime   string `long:"retention" description:"Retention time. Example: 1d, 2h, 5m" default:""`
}

func main() {
	logger := log.New(os.Stdout, "[MetricsBox]", log.LstdFlags)
	var o Options
	parser := flags.NewParser(&o, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	configDir := "../config"
	var conf *config.Config
	switch o.Env {
	case "onebox":
		conf = config.NewConfig(configDir, config.OneBox)
	case "dev":
		conf = config.NewConfig(configDir, config.Dev)
	case "prod":
		conf = config.NewConfig(configDir, config.Prod)
	}

	l, err := ingress.NewLoader(conf, logger)
	CheckError(logger, err)

	if o.CreateDB {
		CheckError(logger, l.CreateDB(o.Database))
		logger.Printf("Database %s created.", o.Database)
	}

	defer func() {
		if o.DropDB {
			l.DropDB(o.Database)
			logger.Printf("Database %s dropped.", o.Database)
		}
	}()

	if len(o.InputFile) > 0 {
		CheckError(logger, l.InsertData(o.InputFile, o.Database, o.Mutations))
		logger.Printf("File %s inserted.", o.InputFile)
	}

	if len(o.Query) > 0 {
		qa, _ := analysis.NewQueryAnalyzer(o.Database, conf, logger)
		var m map[string]float64

		switch o.Query {
		case "elapsed":
			m, err = qa.GetElapsedAvg(o.Days)
		case "cpu":
			m, err = qa.GetTotalWorkerCPUAvg(o.Days)
		case "elapsed_rate":
			m, err = qa.GetElapsedRate(o.Days)
		case "cpu_rate":
			m, err = qa.GetTotalWorkerCPURate(o.Days)
		case "success_rate":
			m, err = qa.GetSuccessRate(o.Days)
		case "ssq_elapsed_host":
			m, err = qa.GetSSQElapsedByHost(o.Days, o.Percentile)
		case "ssq_elapsed_pool":
			m, err = qa.GetSSQElapsedByPool(o.Days, o.Percentile)
		default:
			err = fmt.Errorf("Unrecognized query")
		}

		PrintMap(logger, m)
		CheckError(logger, err)
	}

	if len(o.RetentionPolicy) > 0 {
		CheckError(logger, l.DropRetentionPolicy(o.Database, o.RetentionPolicy))
		if len(o.RetentionTime) > 0 {
			CheckError(logger, l.CreateRetentionPolicy(o.Database, o.RetentionPolicy, o.RetentionTime))
		}
	}
}

func CheckError(logger *log.Logger, err error) {
	if err != nil {
		logger.Fatal(err)
	}
}

func PrintMap(logger *log.Logger, m map[string]float64) {
	if len(m) > 0 {
		logger.Printf("Key\tValue")
		for k, v := range m {
			logger.Printf("%s\t%f", k, v)
		}
	}
}
