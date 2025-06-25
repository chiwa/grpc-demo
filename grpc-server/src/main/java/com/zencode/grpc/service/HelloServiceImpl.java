package com.zengcode.grpc.service;

import com.zengcode.grpc.HelloRequest;
import com.zengcode.grpc.HelloResponse;
import com.zengcode.grpc.HelloServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
@Slf4j
public class HelloServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        String greeting = "Hello, " + request.getName() + " from gRPC Server!";
        log.info(greeting);
        HelloResponse response = HelloResponse.newBuilder()
                .setMessage(greeting)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
