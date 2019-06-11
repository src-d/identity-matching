package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	idmatch "github.com/src-d/eee-identity-matching"
	"github.com/src-d/eee-identity-matching/external"
)

type Args struct {
	Host string
	Port uint
	User string
	Password string
	Output string
	External string
	ApiURL string
	Token string
}

func main() {
	args := parseArgs()

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go func() {
		<-signals
		cancel()
	}()

	logrus.Info("looking for people in commits")
	start := time.Now()

	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		args.User, args.Password, args.Host, args.Port, "gitbase")
	extmatcher, err := external.Matchers[args.External](args.ApiURL, args.Token)
	if err != nil {
		logrus.Fatalf("failed to initialize %s: %v", args.External, err)
	}

	people, err := idmatch.FindPeople(ctx, connStr, path)
	if err != nil {
		logrus.Fatalf("unable to find people: %v", err)
	}

	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
		"people":  len(people),
	}).Info("found people")

	idmatch.MatchByEmail(people)
	logrus.WithField("people", len(people)).Info("grouped people by email")

	idmatch.MatchByGitHub(ctx, people, matcher)
	logrus.WithField("people", len(people)).Info("grouped people by GitHub")

	idmatch.MatchByNames(people)
	logrus.WithField("people", len(people)).Info("grouped people by names")

	if err := storeMatches(people, path); err != nil {
		logrus.Fatalf("unable to store matches: %s", err)
	}

	close(signals)
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
		"enable external service matching, options: " + strings.Join(matchers, ", "))
	flag.StringVar(&args.ApiURL, "api-url", "",
		"API URL of the external matching service, the blank value means the public website")
	flag.StringVar(&args.Token, "token", "", "API token for the external matching service")
	flag.CommandLine.SortFlags = false
	flag.Parse()

	if args.External != "" {
		if _, exists := external.Matchers[args.External]; !exists {
			logrus.Fatalf("unsupported external matching service: %s", args.External)
		}
	}
	return args
}

func storeMatches(people idmatch.People, path string) error {
	var buf bytes.Buffer
	people.Iter(func(_ uint64, p *idmatch.Person) bool {
		buf.WriteString(p.String())
		buf.WriteRune('\n')
		return false
	})

	f, err := os.Create(filepath.Join(path, "identities"))
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, &buf); err != nil {
		return err
	}

	return f.Close()
}
