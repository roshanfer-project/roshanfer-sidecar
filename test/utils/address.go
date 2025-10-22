package utils

import (
	"log"
	"net"
)

// find the ip address from the full address
func GetAddr(fullAddr string) string {
	host, _, err := net.SplitHostPort(fullAddr)
	if err != nil {
		log.Fatalf("Failed to split host and port: %v", err)
	}
	return host
}
