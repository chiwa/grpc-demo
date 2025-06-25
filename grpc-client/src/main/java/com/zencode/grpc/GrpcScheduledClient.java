package com.zencode.grpc;


import com.zengcode.grpc.HelloRequest;
import com.zengcode.grpc.HelloResponse;
import com.zengcode.grpc.HelloServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GrpcScheduledClient {

    private final HelloServiceGrpc.HelloServiceBlockingStub stub;

    public GrpcScheduledClient(@Value("${grpc.server.host}") String grpcHost,
                               @Value("${grpc.server.port}") int grpcPort) {

        ManagedChannel channel = ManagedChannelBuilder
                .forTarget("dns:///" + grpcHost + ":" + grpcPort)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();

        this.stub = HelloServiceGrpc.newBlockingStub(channel);
    }

    @Scheduled(fixedRate = 5000)
    public void callGrpc() {
        HelloRequest request = HelloRequest.newBuilder().setName("พี่พี").build();
        HelloResponse response = stub.sayHello(request);
        log.info("💬 Response from server: {}", response.getMessage());
    }
}