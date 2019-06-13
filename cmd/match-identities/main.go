package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	idmatch "github.com/src-d/eee-identity-matching"
	"github.com/src-d/eee-identity-matching/external"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type Args struct {
	Host     string
	Port     uint
	User     string
	Password string
	Output   string
	External string
	ApiURL   string
	Token    string
	Cache    string
}

func main() {
	args := parseArgs()

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal)
	defer close(signals)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go func() {
		<-signals
		cancel()
	}()

	var extmatcher external.Matcher
	if args.External != "" {
		var err error
		extmatcher, err = external.Matchers[args.External](args.ApiURL, args.Token)
		if err != nil {
			logrus.Fatalf("failed to initialize %s: %v", args.External, err)
		}
	}

	logrus.Info("looking for people in commits")
	start := time.Now()
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		args.User, args.Password, args.Host, args.Port, "gitbase")
	people, err := idmatch.FindPeople(ctx, connStr, args.Cache)
	if err != nil {
		logrus.Fatalf("unable to find people: %v", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"people":  len(people),
	}).Info("found people")

	logrus.Info("reducing people")
	start = time.Now()
	if err := idmatch.ReducePeople(people, extmatcher); err != nil {
		logrus.Fatalf("unable to reduce matches: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"people":  len(people),
	}).Info("reduced people")

	logrus.Info("storing people")
	start = time.Now()
	if err := storeMatches(people, args.Output); err != nil {
		logrus.Fatalf("unable to store matches: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"path":    args.Output,
	}).Info("stored people")
}

func parseArgs() Args {
	var matchers []string
	for key := range external.Matchers {
		matchers = append(matchers, key)
	}
	sort.Strings(matchers)

	args := Args{}
	flag.StringVar(&args.Output, "output", "", "path to the parquet file to write")
	flag.StringVar(&args.Host, "host", "0.0.0.0", "gitbase host")
	flag.UintVar(&args.Port, "port", 3306, "gitbase port")
	flag.StringVar(&args.User, "user", "root", "gitbase user, normally the default value is fine")
	flag.StringVar(&args.Password, "password", "", "gitbase password")
	flag.StringVar(&args.External, "external", "",
		"enable external service matching, options: "+strings.Join(matchers, ", "))
	flag.StringVar(&args.ApiURL, "api-url", "",
		"API URL of the external matching service, the blank value means the public website")
	flag.StringVar(&args.Token, "token", "", "API token for the external matching service")
	flag.StringVar(&args.Cache, "cache", "cache.csv", "Path to the cached raw signatures")
	flag.CommandLine.SortFlags = false
	flag.Parse()

	if args.External != "" {
		if _, exists := external.Matchers[args.External]; !exists {
			logrus.Fatalf("unsupported external matching service: %s", args.External)
		}
	}
	return args
}

func storeMatches(people idmatch.People, path string) (err error) {
	var pf source.ParquetFile
	pf, err = local.NewLocalFileWriter(path)
	defer func() {
		errClose := pf.Close()
		if err == nil {
			err = errClose
		}
		if err != nil {
			logrus.Errorf("failed to store the matches to %s: %v", path, err)
		}
	}()
	var pw *writer.ParquetWriter
	pw, err = writer.NewParquetWriter(pf, new(idmatch.Person), int64(runtime.NumCPU()))
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	people.ForEach(func(key uint64, val *idmatch.Person) bool {
		err = pw.Write(*val)
		return err != nil
	})
	err = pw.WriteStop()
	return
}
