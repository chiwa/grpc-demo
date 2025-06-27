package com.zengcode.grpc.service;

import com.zengcode.grpc.order.OrderRequest;
import com.zengcode.grpc.order.OrderResponse;
import com.zengcode.grpc.order.OrderServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.concurrent.atomic.AtomicInteger;

@GrpcService
@Slf4j
public class OrderServiceImpl extends OrderServiceGrpc.OrderServiceImplBase {

    @Override
    public StreamObserver<OrderRequest> processOrders(StreamObserver<OrderResponse> responseObserver) {
        AtomicInteger totalOrders = new AtomicInteger(0);

        return new StreamObserver<>() {
            @Override
            public void onNext(OrderRequest order) {
                log.info("Received Order: {} x{}", order.getProduct(), order.getQuantity());
                totalOrders.incrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error receiving order stream", t);
            }

            @Override
            public void onCompleted() {
                OrderResponse response = OrderResponse.newBuilder()
                        .setTotalOrders(totalOrders.get())
                        .setStatus("Orders Processed Successfully")
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                log.info("Finished processing {} orders", totalOrders.get());
            }
        };
    }
}