package com.zengcode.grpc.service;

import com.zengcode.grpc.HelloRequest;
import com.zengcode.grpc.HelloResponse;
import com.zengcode.grpc.HelloServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
@Slf4j
@RequiredArgsConstructor
public class HelloServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {

    private final MeterRegistry meterRegistry;

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        meterRegistry.counter("grpc_requests_total", "method", "sayHello").increment();
        String greeting = "Hello, " + request.getName() + " from gRPC Server!";
        log.info(greeting);
        HelloResponse response = HelloResponse.newBuilder()
                .setMessage(greeting)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
