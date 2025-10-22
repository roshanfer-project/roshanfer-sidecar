package utils

import (
	"os"
	"strconv"

	"google.golang.org/grpc/grpclog"
)

var logger = grpclog.Component("EnvVars")

func GetEnvVar(key string, required bool) string {
	value := os.Getenv(key)
	if value == "" && required {
		logger.Fatalf("Environment variable %s is required", key)
	}
	return value
}

func StrToInt(s string) int {
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		logger.Fatalf("Failed to convert string to int: %s", err)
		return 0
	} else {
		return int(i)
	}
}

func StrToFloat64(s string) float64 {
	if f, err := strconv.ParseFloat(s, 64); err != nil {
		logger.Fatalf("Failed to convert string to float64: %s", err)
		return 0
	} else {
		return f
	}
}
