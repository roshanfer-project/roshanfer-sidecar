package test

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var Opts = []grpc.ServerOption{
	grpc.KeepaliveParams(keepalive.ServerParameters{
		Timeout: 120 * time.Second,
	}),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		PermitWithoutStream: true,
	}),
	//grpc.UnaryInterceptor(tracingInterceptor),
	grpc.WriteBufferSize(0),
}
