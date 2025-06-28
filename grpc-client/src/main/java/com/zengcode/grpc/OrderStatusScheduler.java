package com.zengcode.grpc;

import com.zengcode.grpc.order.OrderServiceGrpc;
import com.zengcode.grpc.orderstatus.OrderStatusServiceGrpc;
import com.zengcode.grpc.orderstatus.Orderstatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderStatusScheduler {

    private final ManagedChannel channel;
    private OrderStatusServiceGrpc.OrderStatusServiceBlockingStub stub;

    public OrderStatusScheduler(@Value("${grpc.server.host}") String grpcHost,
                                @Value("${grpc.server.port}") int grpcPort) {
        this.channel = ManagedChannelBuilder
                .forAddress(grpcHost, grpcPort) // ✅ สำหรับ Docker ใช้ชื่อ service
                .usePlaintext()
                .build();

    }

    @Scheduled(fixedRate = 30_000) // ทุก 30 วินาที
    public void sendBatchOrders() {
        stub = OrderStatusServiceGrpc.newBlockingStub(channel);
        Orderstatus.OrderStatusRequest request = Orderstatus.OrderStatusRequest.newBuilder().setOrderId("B001").build();
        stub.getOrderStatus(request).forEachRemaining(res -> {
            log.info("📦 Status update: {}, {}", res.getStatus(), res.getTimestamp());
        });
        log.info("==========================================");
    }
}

/**
 package com.zengcode.grpc;

 import com.zengcode.grpc.orderstatus.OrderStatusServiceGrpc;
 import com.zengcode.grpc.orderstatus.Orderstatus;
 import io.grpc.ManagedChannel;
 import io.grpc.ManagedChannelBuilder;
 import io.grpc.stub.StreamObserver;
 import lombok.extern.slf4j.Slf4j;
 import org.springframework.beans.factory.annotation.Value;
 import org.springframework.scheduling.annotation.Scheduled;
 import org.springframework.stereotype.Component;

 @Component
 @Slf4j
 public class OrderStatusAsyncScheduler {

 private final ManagedChannel channel;
 private final OrderStatusServiceGrpc.OrderStatusServiceStub stub;

 public OrderStatusAsyncScheduler(@Value("${grpc.server.host}") String grpcHost,
 @Value("${grpc.server.port}") int grpcPort) {
 this.channel = ManagedChannelBuilder
 .forAddress(grpcHost, grpcPort)
 .usePlaintext()
 .build();

 this.stub = OrderStatusServiceGrpc.newStub(channel);
 }

 @Scheduled(fixedRate = 30_000) // ทุก 30 วินาที
 public void fetchStatusAsync() {
 Orderstatus.OrderStatusRequest request = Orderstatus.OrderStatusRequest.newBuilder()
 .setOrderId("B001")
 .build();

 stub.getOrderStatus(request, new StreamObserver<>() {
 @Override
 public void onNext(Orderstatus.OrderStatusResponse response) {
 log.info("📦 [Async] Status update: {}, {}", response.getStatus(), response.getTimestamp());
 }

 @Override
 public void onError(Throwable t) {
 log.error("❌ [Async] Error during order status stream", t);
 }

 @Override
 public void onCompleted() {
 log.info("✅ [Async] Order status stream completed");
 log.info("==========================================");
 }
 });
 }
 }
 */