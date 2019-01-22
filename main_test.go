package grpcunavailable

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:generate protoc --go_out=plugins=grpc:. service.proto

func TestStop(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatalf("failed to create listener: %+v", err)
	}

	srv := grpc.NewServer()

	requestStartedBr, requestDoneBr := make(barrier), make(barrier)
	RegisterUnavailableServer(srv, unavailableServer(func(ctx context.Context, r *RunRequest) (*RunResponse, error) {
		defer requestDoneBr.done()
		requestStartedBr.done()
		<-ctx.Done()
		// the code here doesn't matter because it will be returned after server `Stop`.
		return nil, status.Error(codes.Internal, "")
	}))

	serverDoneBr := make(barrier)
	go func() {
		defer serverDoneBr.done()
		err := srv.Serve(listener)
		if err != nil {
			t.Errorf("server serve returned error: %+v", err)
		}
	}()

	clientConn, err := grpc.Dial(listener.Addr().String(), grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("failed to create client connection: %+v", err)
	}
	defer func() {
		err := clientConn.Close()
		if err != nil {
			t.Errorf("client close failed: %+v", err)
		}
	}()

	cli := NewUnavailableClient(clientConn)

	clientDoneBr := make(barrier)
	go func() {
		defer clientDoneBr.done()
		_, err := cli.Run(context.Background(), &RunRequest{})
		if err == nil {
			t.Errorf("error expected")
			return
		}

		// Excerpt from `codes` documentation (see https://github.com/grpc/grpc-go/blob/master/codes/codes.go#L83):
		// -------------------------
		//  ...
		// A litmus test that may help a service implementor in deciding
		// between FailedPrecondition, Aborted, and Unavailable:
		//  (a) Use Unavailable if the client can retry just the failing call.
		//  ...
		// -------------------------
		//
		// So according to the docs if I get `Unavailable` here I should be able to safely retry the request.
		// But I can't safely retry the request here because it could be fully executed at server.
		if status.Code(err) == codes.Unavailable {
			t.Errorf("status code shouldn't be codes.Unavailable")
		}
	}()

	// we ensure here that the request has already reached the handler
	waitBarriers(t, requestStartedBr)

	// stop the server
	// request handler will be cancelled using `context.Context`
	// it doesn't matter which status will be returned from the handler after `Stop`
	// the client should carefully handle server shutdown
	srv.Stop()

	waitBarriers(t, requestDoneBr, clientDoneBr, serverDoneBr)
}

type barrier chan struct{}
type unavailableServer func(ctx context.Context, r *RunRequest) (*RunResponse, error)

func (fn unavailableServer) Run(ctx context.Context, r *RunRequest) (*RunResponse, error) {
	return fn(ctx, r)
}

func (ch barrier) done() {
	close(ch)
}

func waitBarriers(t *testing.T, brs ...barrier) {
	timeout := time.After(10 * time.Second)
	for _, br := range brs {
		select {
		case <-br:
		case <-timeout:
			t.Fatal("timed out")
		}
	}
}
