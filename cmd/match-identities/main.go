package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	idmatch "github.com/src-d/eee-identity-matching"
	"github.com/src-d/eee-identity-matching/external"
	"github.com/src-d/eee-identity-matching/reporter"
)

type cliArgs struct {
	Host          string
	Port          uint
	User          string
	Password      string
	Output        string
	External      string
	APIURL        string
	Token         string
	Cache         string
	ExternalCache string
	MaxIdentities int
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
		extmatcher, err = external.Matchers[args.External](args.APIURL, args.Token)
		if err != nil {
			logrus.Fatalf("failed to initialize %s: %v", args.External, err)
		}
		if args.ExternalCache != "" {
			extmatcher, err = external.NewCachedMatcher(extmatcher, args.ExternalCache)
			if err != nil {
				logrus.Fatalf("failed to initialize cached %s: %v", args.External, err)
			}
		}
	}

	logrus.Info("looking for people in commits")
	start := time.Now()
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		args.User, args.Password, args.Host, args.Port, "gitbase")
	blacklist, err := idmatch.NewBlacklist()
	if err != nil {
		logrus.Fatalf("unable to load the blacklist: %v", err)
	}
	people, nameFreqs, err := idmatch.FindPeople(ctx, connStr, args.Cache, blacklist)
	if err != nil {
		logrus.Fatalf("unable to find people: %v", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"people":  len(people),
	}).Info("found people")

	logrus.Info("reducing people")
	start = time.Now()
	if err := idmatch.ReducePeople(people, extmatcher, blacklist, args.MaxIdentities); err != nil {
		logrus.Fatalf("unable to reduce matches: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"people":  len(people),
	}).Info("reduced people")

	start = time.Now()
	idmatch.SetPrimaryName(people, nameFreqs)
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
	}).Info("primary names are set")

	logrus.Info("storing people")
	start = time.Now()
	if err := people.WriteToParquet(args.Output, args.External); err != nil {
		logrus.Fatalf("unable to store matches: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"path":    args.Output,
	}).Info("stored people")

	reporter.Write()
}

func parseArgs() cliArgs {
	var matchers []string
	for key := range external.Matchers {
		matchers = append(matchers, key)
	}
	sort.Strings(matchers)

	args := cliArgs{}
	flag.StringVar(&args.Output, "output", "", "path to the parquet file to write")
	flag.StringVar(&args.Host, "host", "0.0.0.0", "gitbase host")
	flag.UintVar(&args.Port, "port", 3306, "gitbase port")
	flag.StringVar(&args.User, "user", "root", "gitbase user, normally the default value is fine")
	flag.StringVar(&args.Password, "password", "", "gitbase password")
	flag.StringVar(&args.External, "external", "",
		"enable external service matching, options: "+strings.Join(matchers, ", "))
	flag.StringVar(&args.APIURL, "api-url", "",
		"API URL of the external matching service, the blank value means the public website")
	flag.StringVar(&args.Token, "token", "", "API token for the external matching service")
	flag.StringVar(&args.Cache, "cache", "cache-raw.csv", "Path to the cached raw signatures")
	flag.StringVar(&args.ExternalCache, "external-cache", "cache-external.csv",
		"Path to the cached matches found by using an external identity service such as GitHub API.")
	flag.IntVar(&args.MaxIdentities, "max-identities", 20,
		"If a person has more than this number of unique names and unique emails summed, "+
			"no more identities will be merged. If the identities are matched by an external API "+
			"or by email this limitation can be violated.")
	flag.CommandLine.SortFlags = false
	flag.Parse()

	if args.External != "" {
		if _, exists := external.Matchers[args.External]; !exists {
			logrus.Fatalf("unsupported external matching service: %s", args.External)
		}
	}
	return args
}
