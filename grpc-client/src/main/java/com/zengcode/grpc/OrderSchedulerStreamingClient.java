package com.zengcode.grpc;

import com.zengcode.grpc.order.OrderRequest;
import com.zengcode.grpc.order.OrderResponse;
import com.zengcode.grpc.order.OrderServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class OrderSchedulerStreamingClient {

    private final ManagedChannel channel;
    private final OrderServiceGrpc.OrderServiceStub stub;

    public OrderSchedulerStreamingClient(@Value("${grpc.server.host}") String grpcHost,
                                         @Value("${grpc.server.port}") int grpcPort) {
        this.channel = ManagedChannelBuilder
                .forAddress(grpcHost, grpcPort) // ✅ สำหรับ Docker ใช้ชื่อ service
                .usePlaintext()
                .build();

        this.stub = OrderServiceGrpc.newStub(channel);
    }

    //@Scheduled(fixedRate = 30_000) // ทุก 30 วินาที
    public void sendBatchOrders() {
        log.info("🚀 Starting new batch stream...");

        StreamObserver<OrderRequest> requestObserver = stub.processOrders(new StreamObserver<>() {
            @Override
            public void onNext(OrderResponse value) {
                log.info("Server response: {},  {}",value.getTotalOrders(), value.getStatus());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error from server", t);
            }

            @Override
            public void onCompleted() {
                log.info("Streaming completed.");
            }
        });

        List<OrderRequest> orders = List.of(
                OrderRequest.newBuilder().setOrderId("B001").setProduct("Phone").setQuantity(1).build(),
                OrderRequest.newBuilder().setOrderId("B002").setProduct("Television").setQuantity(2).build(),
                OrderRequest.newBuilder().setOrderId("B003").setProduct("Laptop").setQuantity(1).build()
        );

        orders.forEach(requestObserver::onNext);
        requestObserver.onCompleted();
    }
}