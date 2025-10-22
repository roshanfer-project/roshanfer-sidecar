package main

import (
	"context"
	"fmt"
	"net"
	"test"
	"test/protobuf"
	"test/utils"
	"time"

	"google.golang.org/grpc"
)

const serviceName = "backend1"

var log = utils.GetLogger(serviceName)

type Backend1Server struct {
	protobuf.UnimplementedBackend1Server
}

func (s *Backend1Server) SimpleCall(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	time.Sleep(2 * time.Millisecond) // simulate some processing delay
	resp := &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}
	return resp, nil
}

func (s *Backend1Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	time.Sleep(2 * time.Millisecond) // simulate some processing delay
	return &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}, nil
}

func (s *Backend1Server) Run() error {

	opts := test.Opts

	srv := grpc.NewServer(opts...)
	protobuf.RegisterBackend1Server(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", utils.StrToInt(utils.GetEnvVar("Backend1Port", true))))
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
