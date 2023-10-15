package scalingo

import (
	"bufio"
	"io"
)

type HttpSyslogMessage struct {
	Content         []byte
	Path            string
}

// Reads log messages in the Syslog TCP protocol octet counting framing method
//
// See format documentation here:
// - https://devcenter.heroku.com/articles/log-drains#https-drains
// - https://datatracker.ietf.org/doc/html/rfc6587#section-3.4.1
//
// Discards all syslog messages not related to Heroku Postgres
func ReadScalingoPostgresMessages(r io.Reader) []HttpSyslogMessage {
	var out []HttpSyslogMessage

	reader := bufio.NewReader(r)
	for {
		content, err := io.ReadAll(reader)
		if err != nil {
			break
		}

		item := HttpSyslogMessage{
			Content:         content,
			Path:            "", // To be added later by caller
		}
		out = append(out, item)
	}

	return out
}
