package utils

import (
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
)

// prefixedWriter wraps an io.Writer and prepends a character to every log line.
type prefixedWriter struct {
	prefix string
	writer io.Writer
}

func (pw *prefixedWriter) Write(p []byte) (n int, err error) {
	lines := strings.Split(string(p), "\n")
	var out string
	for _, line := range lines {
		if line != "" {
			out += pw.prefix + line + "\n"
		}
	}
	return pw.writer.Write([]byte(out))
}

func GetLoggerWithTag(name string) *slog.Logger {
	// Read the LOG_LEVEL environment variable
	logLevel := os.Getenv("LOG_LEVEL")

	// Default to INFO if LOG_LEVEL is not set or invalid
	level := slog.LevelInfo

	// Parse the LOG_LEVEL environment variable
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		log.Printf("Invalid LOG_LEVEL '%s', defaulting to INFO\n", logLevel)
	}

	// Create a prefixed writer that prepends ">" to each log line.
	pw := &prefixedWriter{
		prefix: "M# ",
		writer: os.Stdout,
	}

	// Create a new logger with the desired level and custom writer.
	logger := slog.New(slog.NewTextHandler(pw, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// check that we are handling the time key
			if a.Key != slog.TimeKey {
				return a
			}

			t := a.Value.Time()

			// change the value from a time.Time to a String
			// where the string has the correct time format.
			a.Value = slog.StringValue(t.Format("03:04:05.000000"))

			return a
		},
	}))

	return logger.With("package", name)
}

func GetLogger(name string) *slog.Logger {
	// Read the LOG_LEVEL environment variable
	logLevel := os.Getenv("LOG_LEVEL")

	// Default to INFO if LOG_LEVEL is not set or invalid
	level := slog.LevelInfo

	// Parse the LOG_LEVEL environment variable
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		log.Printf("Invalid LOG_LEVEL '%s', defaulting to INFO\n", logLevel)
	}

	// Create a new logger with the desired level
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// check that we are handling the time key
			if a.Key != slog.TimeKey {
				return a
			}

			t := a.Value.Time()

			// change the value from a time.Time to a String
			// where the string has the correct time format.
			a.Value = slog.StringValue(t.Format("04:05.000000"))

			return a
		},
	}))

	return logger.With("package", name)
}
