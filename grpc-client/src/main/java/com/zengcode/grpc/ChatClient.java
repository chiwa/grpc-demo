package com.zengcode.grpc;

import com.zengcode.grpc.chat.ChatMessage;
import com.zengcode.grpc.chat.ChatServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@Component
@Slf4j
public class ChatClient {

    private final ManagedChannel channel;
    public ChatClient(@Value("${grpc.server.host}") String grpcHost,
                                @Value("${grpc.server.port}") int grpcPort) {
        this.channel = ManagedChannelBuilder
                .forAddress(grpcHost, grpcPort) // ✅ สำหรับ Docker ใช้ชื่อ service
                .usePlaintext()
                .build();
    }

    @PostConstruct
    public void init() {
        startClient("client-a");
        startClient("client-b");
    }

    private  void startClient(String clientId) {
        ChatServiceGrpc.ChatServiceStub stub = ChatServiceGrpc
                .newStub(channel)
                .withWaitForReady();

        StreamObserver<ChatMessage> requestObserver = stub.chat(new StreamObserver<>() {
            @Override
            public void onNext(ChatMessage value) {
                log.info("{} 💬{}", clientId, value.getContent());
            }

            @Override
            public void onError(Throwable t) {
                log.info("{} ❌ Error: {}", clientId, t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("{} ✅ Chat ended", clientId);
            }
        });

        AtomicInteger counter = new AtomicInteger(1);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            String content = "Hello from " + clientId + ", Message " + counter.getAndIncrement();
            ChatMessage message = ChatMessage.newBuilder()
                    .setClientId(clientId)
                    .setContent(content)
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build();
            requestObserver.onNext(message);
        }, 0, 5, TimeUnit.SECONDS);
    }
}
