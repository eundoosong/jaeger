// Copyright (c) 2019,2020 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/opentracing/opentracing-go"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/jaegertracing/jaeger/cmd/query/app/querysvc"
	"github.com/jaegertracing/jaeger/pkg/healthcheck"
	"github.com/jaegertracing/jaeger/pkg/netutils"
	"github.com/jaegertracing/jaeger/pkg/recoveryhandler"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
)

// Server runs HTTP, Mux and a grpc server
type Server struct {
	logger       *zap.Logger
	querySvc     *querysvc.QueryService
	queryOptions *QueryOptions

	tracer opentracing.Tracer // TODO make part of flags.Service

	conn               net.Listener
	grpcServer         *grpc.Server
	httpServer         *http.Server
	unavailableChannel chan healthcheck.Status
}

// NewServer creates and initializes Server
func NewServer(logger *zap.Logger, querySvc *querysvc.QueryService, options *QueryOptions, tracer opentracing.Tracer) (*Server, error) {
	grpcServer, err := createGRPCServer(querySvc, options, logger, tracer)
	if err != nil {
		return nil, err
	}

	return &Server{
		logger:             logger,
		querySvc:           querySvc,
		queryOptions:       options,
		tracer:             tracer,
		grpcServer:         grpcServer,
		httpServer:         createHTTPServer(querySvc, options, tracer, logger),
		unavailableChannel: make(chan healthcheck.Status),
	}, nil
}

// HealthCheckStatus returns health check status channel a client can subscribe to
func (s Server) HealthCheckStatus() chan healthcheck.Status {
	return s.unavailableChannel
}

func createGRPCServer(querySvc *querysvc.QueryService, options *QueryOptions, logger *zap.Logger, tracer opentracing.Tracer) (*grpc.Server, error) {
	var grpcOpts []grpc.ServerOption

	if options.TLS.Enabled {
		tlsCfg, err := options.TLS.Config()
		if err != nil {
			return nil, err
		}
		creds := credentials.NewTLS(tlsCfg)

		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}

	server := grpc.NewServer(grpcOpts...)

	handler := NewGRPCHandler(querySvc, logger, tracer)
	api_v2.RegisterQueryServiceServer(server, handler)
	return server, nil
}

func createHTTPServer(querySvc *querysvc.QueryService, queryOpts *QueryOptions, tracer opentracing.Tracer, logger *zap.Logger) (*http.Server, error) {
	var tlsCfg *tls.Config
	if queryOpts.TLS.Enabled {
		tlsCfg, err := queryOpts.TLS.Config()
		if err != nil {
			return nil, err
		}
	}

	apiHandlerOptions := []HandlerOption{
		HandlerOptions.Logger(logger),
		HandlerOptions.Tracer(tracer),
	}
	apiHandler := NewAPIHandler(
		querySvc,
		apiHandlerOptions...)
	r := NewRouter()
	if queryOpts.BasePath != "/" {
		r = r.PathPrefix(queryOpts.BasePath).Subrouter()
	}

	apiHandler.RegisterRoutes(r)
	RegisterStaticHandler(r, logger, queryOpts)
	var handler http.Handler = r
	handler = additionalHeadersHandler(handler, queryOpts.AdditionalHeaders)
	if queryOpts.BearerTokenPropagation {
		handler = bearerTokenPropagationHandler(logger, handler)
	}
	handler = handlers.CompressHandler(handler)
	recoveryHandler := recoveryhandler.NewRecoveryHandler(logger, true)

	return &http.Server{
		Handler:   recoveryHandler(handler),
		TLSConfig: tlsCfg,
	}, nil
}

// Start http, GRPC and cmux servers concurrently
func (s *Server) Start() error {
	conn, err := net.Listen("tcp", s.queryOptions.HostPort)
	if err != nil {
		return err
	}
	s.conn = conn

	var tcpPort int
	if port, err := netutils.GetPort(s.conn.Addr()); err == nil {
		tcpPort = port
	}

	s.logger.Info(
		"Query server started",
		zap.Int("port", tcpPort),
		zap.String("addr", s.queryOptions.HostPort))

	// cmux server acts as a reverse-proxy between HTTP and GRPC backends.
	cmuxServer := cmux.New(s.conn)

	grpcListener := cmuxServer.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"),
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc+proto"),
	)
	httpListener := cmuxServer.Match(cmux.Any())

	go func() {
		s.logger.Info("Starting HTTP server", zap.Int("port", tcpPort), zap.String("addr", s.queryOptions.HostPort))

		tlsCfg, err := s.queryOptions.TLS.Config()
		if err != nil {
			return nil, err
		}
		// https://github.com/soheilhy/cmux/blob/8a8ea3c53959009183d7914522833c1ed8835020/example_tls_test.go#L57
		tlsl := tls.NewListener(httpListener, tlsCfg)
		switch err := s.httpServer.Serve(tlsl); err {
		case nil, http.ErrServerClosed, cmux.ErrListenerClosed:
			// normal exit, nothing to do
		default:
			s.logger.Error("Could not start HTTP server", zap.Error(err))
		}
		s.unavailableChannel <- healthcheck.Unavailable
	}()

	// Start GRPC server concurrently
	go func() {
		s.logger.Info("Starting GRPC server", zap.Int("port", tcpPort), zap.String("addr", s.queryOptions.HostPort))

		if err := s.grpcServer.Serve(grpcListener); err != nil {
			s.logger.Error("Could not start GRPC server", zap.Error(err))
		}
		s.unavailableChannel <- healthcheck.Unavailable
	}()

	// Start cmux server concurrently.
	go func() {
		s.logger.Info("Starting CMUX server", zap.Int("port", tcpPort), zap.String("addr", s.queryOptions.HostPort))

		err := cmuxServer.Serve()
		// TODO: Remove string comparison when https://github.com/soheilhy/cmux/pull/69 is merged
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			s.logger.Error("Could not start multiplexed server", zap.Error(err))
		}
		s.unavailableChannel <- healthcheck.Unavailable
	}()

	return nil
}

// Close stops http, GRPC servers and closes the port listener.
func (s *Server) Close() {
	s.grpcServer.Stop()
	s.httpServer.Close()
	s.conn.Close()
}
