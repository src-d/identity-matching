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

	idmatch "github.com/src-d/identity-matching"
	"github.com/src-d/identity-matching/external"
	"github.com/src-d/identity-matching/reporter"
)

type cliArgs struct {
	Host           string
	Port           uint
	User           string
	Password       string
	Output         string
	External       string
	APIURL         string
	Token          string
	Cache          string
	ExternalCache  string
	MaxIdentities  int
	RecentMonths   int
	RecentMinCount int
}

var version string
var build string
var commit string

func printBanner() {
	fmt.Println(strings.Repeat("=", 80))

	wrap := func(s string) string {
		return s + strings.Repeat(" ", 80-len(s)-1) + "="
	}

	fmt.Println(wrap("= src-d/identity-matching " + version))
	fmt.Println(wrap("= git " + commit))
	fmt.Println(wrap("= built on " + build))
	fmt.Println(strings.Repeat("=", 80))
}

func main() {
	printBanner()
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

	logrus.Info("fetching signatures from the commits")
	start := time.Now()
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		args.User, args.Password, args.Host, args.Port, "gitbase")
	blacklist, err := idmatch.NewBlacklist()
	if err != nil {
		logrus.Fatalf("failed to load the blacklist: %v", err)
	}
	people, nameFreqs, emailFreqs, err := idmatch.FindPeople(ctx, connStr, args.Cache, blacklist,
		args.RecentMonths)
	if err != nil {
		logrus.Fatalf("failed to fetch the signatures: %v", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"count":   len(people),
	}).Info("found signatures")

	logrus.Info("reducing identities")
	start = time.Now()
	if err := idmatch.ReducePeople(people, extmatcher, blacklist, args.MaxIdentities); err != nil {
		logrus.Fatalf("failed to reduce identities: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"count":   len(people),
	}).Info("reduced identities")

	start = time.Now()
	idmatch.SetPrimaryValues(people, nameFreqs, emailFreqs, args.RecentMinCount)
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
	}).Info("set primary names and emails")

	logrus.Info("storing identities")
	start = time.Now()
	if err := people.WriteToParquet(args.Output, args.External); err != nil {
		logrus.Fatalf("failed to store identities: %s", err)
	}
	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"path":    args.Output,
	}).Info("stored identities")

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
	flag.StringVar(&args.Cache, "cache", fmt.Sprintf("cache-raw-%s.csv", idmatch.HashPeopleDiscoverySQL()),
		"Path to the cached raw signatures")
	flag.StringVar(&args.ExternalCache, "external-cache", "cache-external-{provider}.csv",
		"Path to the cached matches found by using an external identity service such as GitHub API."+
			"{provider} will be replaced with the external service name.")
	flag.IntVar(&args.MaxIdentities, "max-identities", 20,
		"If a person has more than this number of unique names and unique emails summed, "+
			"no more identities will be merged. If the identities are matched by an external API "+
			"or by email this limitation can be violated.")
	flag.IntVar(&args.RecentMonths, "months", 12,
		"Number of preceding months to consider while calculating stats for detecting "+
			"the primary names and emails.")
	flag.IntVar(&args.RecentMinCount, "min-count", 5,
		"Minimum total number of commits the identity should have in the last --months so that "+
			"the corresponding stats are used for detecting the primary names and emails. "+
			"Otherwise, the stats collected through all the time will be used.")
	flag.CommandLine.SortFlags = false
	flag.Parse()

	if args.External != "" {
		if _, exists := external.Matchers[args.External]; !exists {
			logrus.Fatalf("unsupported external matching service: %s", args.External)
		}
	}
	args.ExternalCache = strings.ReplaceAll(args.ExternalCache, "{provider}", args.External)
	return args
}
