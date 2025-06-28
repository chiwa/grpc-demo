package com.zengcode.grpc.service;

import com.zengcode.grpc.orderstatus.OrderStatusServiceGrpc;
import com.zengcode.grpc.orderstatus.Orderstatus;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.LocalDateTime;
import java.util.List;

//@GrpcService
@Slf4j
public class OrderStatusServiceImpl extends OrderStatusServiceGrpc.OrderStatusServiceImplBase {
    @Override
    public void getOrderStatus(Orderstatus.OrderStatusRequest request, StreamObserver<Orderstatus.OrderStatusResponse> responseObserver) {
        List<String> statuses = List.of("RECEIVED", "PROCESSING", "SHIPPED", "DELIVERED");
        for (String status : statuses) {
            log.info("Current status = {}", status);
            Orderstatus.OrderStatusResponse response = Orderstatus.OrderStatusResponse.newBuilder()
                    .setStatus(status)
                    .setTimestamp(LocalDateTime.now().toString())
                    .build();
            responseObserver.onNext(response);
            try {
                Thread.sleep(1000); // จำลอง delay
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        responseObserver.onCompleted();
    }
}