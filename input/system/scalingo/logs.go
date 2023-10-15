package scalingo

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kr/logfmt"
	"github.com/pganalyze/collector/grant"
	"github.com/pganalyze/collector/logs"
	"github.com/pganalyze/collector/output"
	"github.com/pganalyze/collector/state"
	"github.com/pganalyze/collector/util"
)

type SystemSample struct {
	Source            string  `logfmt:"source"`
	LoadAvg1min       float64 `logfmt:"sample#load-avg-1m"`
	LoadAvg5min       float64 `logfmt:"sample#load-avg-5m"`
	LoadAvg15min      float64 `logfmt:"sample#load-avg-15m"`
	MemoryPostgresKb  string  `logfmt:"sample#memory-postgres"`
	MemoryTotalUsedKb string  `logfmt:"sample#memory-total"`
	MemoryFreeKb      string  `logfmt:"sample#memory-free"`
	MemoryCachedKb    string  `logfmt:"sample#memory-cached"`
	StorageBytesUsed  string  `logfmt:"sample#db_size"`
	ReadIops          float64 `logfmt:"sample#read-iops"`
	WriteIops         float64 `logfmt:"sample#write-iops"`
}

func catchIdentifyServerLine(sourceName string, content string, sourceToServer map[string]*state.Server, servers []*state.Server) map[string]*state.Server {
	identifyParts := regexp.MustCompile(`^pganalyze-collector-identify: ([\w_]+)`).FindStringSubmatch(content)
	if len(identifyParts) == 2 {
		for _, server := range servers {
			if server.Config.SectionName == identifyParts[1] {
				sourceToServer[sourceName] = server
			}
		}
	}

	return sourceToServer
}

func logStreamItemToLogLine(ctx context.Context, item HttpSyslogMessage, servers []*state.Server, sourceToServer map[string]*state.Server, now time.Time, globalCollectionOpts state.CollectionOpts, logger *util.Logger) (map[string]*state.Server, *state.LogLine, string) {
	sourceName := item.Path
	if strings.HasPrefix(item.Path, "/logs/") {
		sourceName = strings.Replace(item.Path, "/logs/", "", 1)
	}

	logLine, _ := logs.ParseLogLineWithPrefix("", item.Content+"\n", nil)
	sourceToServer = catchIdentifyServerLine(sourceName, logLine.Content, sourceToServer, servers)

	return sourceToServer, &logLine, sourceName
}

func setupLogTransformer(ctx context.Context, wg *sync.WaitGroup, servers []*state.Server, in <-chan HttpSyslogMessage, out chan state.ParsedLogStreamItem, globalCollectionOpts state.CollectionOpts, logger *util.Logger) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		sourceToServer := make(map[string]*state.Server)

		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}

				now := time.Now()

				var logLine *state.LogLine
				var sourceName string
				sourceToServer, logLine, sourceName = logStreamItemToLogLine(ctx, item, servers, sourceToServer, now, globalCollectionOpts, logger)
				if logLine == nil || sourceName == "" {
					continue
				}

				server, exists := sourceToServer[sourceName]
				if !exists {
					logger.PrintInfo("Ignoring log line since server can't be matched yet - if this keeps showing up you have a configuration error for %s", sourceName)
					continue
				}

				logLine.Username = server.Config.GetDbUsername()
				logLine.Database = server.Config.GetDbName()
				out <- state.ParsedLogStreamItem{Identifier: server.Config.Identifier, LogLine: *logLine}
			}
		}
	}()
}
