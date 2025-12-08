package main

import (
	"context"
	"fmt"
	"net"
	"test"
	"test/protobuf"
	"test/utils"

	"google.golang.org/grpc"
)

const serviceName = "backend1-b"

var log = utils.GetLogger(serviceName)
var repeat int

func init() {
	repeat = utils.StrToInt(utils.GetEnvVar(serviceName+"-repeat", true))
}

type Backend1Server struct {
	protobuf.UnimplementedBackend1Server
}

func busyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func (s *Backend1Server) SimpleCall(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(repeat) // simulate some processing delay
	resp := &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}
	return resp, nil
}

func (s *Backend1Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(repeat) // simulate some processing delay
	return &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}, nil
}

func (s *Backend1Server) Run() error {

	opts := test.Opts

	srv := grpc.NewServer(opts...)
	protobuf.RegisterBackend1Server(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", utils.StrToInt(utils.GetEnvVar(serviceName+"-listen-port", true))))
	if err != nil {
		log.Error(fmt.Sprintf("failed to listen: %v", err))
	}

	return srv.Serve(lis)
}

func main() {
	s := &Backend1Server{}
	log.Info("Starting backend1 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend1 server", err)
	}
}
