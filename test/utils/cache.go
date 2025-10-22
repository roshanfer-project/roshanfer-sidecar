package utils

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

var defaultMemCMaxIdleConns int = 512
var defaultMemCTimeout int = 2

func NewMemCClient2(servers string) *memcache.Client {
	ss := new(memcache.ServerList)
	server_list := strings.Split(servers, ",")
	err := ss.SetServers(server_list...)
	if err != nil {
		// Hack: panic early to avoid pod restart during running
		panic(err)
		//return nil, err
	} else {
		memc_client := memcache.NewFromSelector(ss)
		memc_client.Timeout = time.Second * time.Duration(GetMemCTimeout())
		memc_client.MaxIdleConns = defaultMemCMaxIdleConns
		return memc_client
	}
}

func GetMemCTimeout() int {
	timeout := defaultMemCTimeout
	if val, ok := os.LookupEnv("MEMC_TIMEOUT"); ok {
		timeout, _ = strconv.Atoi(val)
	}
	logger.Infof("Tune: GetMemCTimeout %d", timeout)
	return timeout
}
