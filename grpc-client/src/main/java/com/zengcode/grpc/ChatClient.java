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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


@Component
@Slf4j
public class ChatClient {

    private final AtomicBoolean isClientBAlive = new AtomicBoolean(false);
    private final AtomicBoolean isReconnectScheduled = new AtomicBoolean(false);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private final ManagedChannel channel;

    public ChatClient(@Value("${grpc.server.host}") String grpcHost,
                      @Value("${grpc.server.port}") int grpcPort) {
        this.channel = ManagedChannelBuilder
                .forAddress(grpcHost, grpcPort)
                .usePlaintext()
                .build();
    }

    @PostConstruct
    public void init() {
        startClient("client-a");
        startClient("client-b");
    }

    private void startClient(String clientId) {
        ChatServiceGrpc.ChatServiceStub stub = ChatServiceGrpc
                .newStub(channel)
                .withWaitForReady();

        StreamObserver<ChatMessage> requestObserver = stub.chat(new StreamObserver<>() {
            @Override
            public void onNext(ChatMessage value) {
                log.info("{} 💬 {}", clientId, value.getContent());
            }

            @Override
            public void onError(Throwable t) {
                log.warn("{} ❌ Error: {}", clientId, t.getMessage());
                if (clientId.equals("client-b")) {
                    isClientBAlive.set(false);
                    scheduleReconnect();
                }
            }

            @Override
            public void onCompleted() {
                log.info("{} ✅ Chat ended", clientId);
                if (clientId.equals("client-b")) {
                    isClientBAlive.set(false);
                    scheduleReconnect();
                }
            }
        });

        AtomicInteger counter = new AtomicInteger(1);

        executor.scheduleAtFixedRate(() -> {
            String content = "Hello from " + clientId + ", Message " + counter.getAndIncrement();
            ChatMessage message = ChatMessage.newBuilder()
                    .setClientId(clientId)
                    .setContent(content)
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build();
            requestObserver.onNext(message);

            if (clientId.equals("client-b") && counter.get() > 10) {
                log.info("{} 🔚 Sent 10 messages, disconnecting...", clientId);
                Thread.currentThread().stop();
                //requestObserver.onCompleted();
            }

        }, 0, 5, TimeUnit.SECONDS);

        // mark client-b as alive
        if (clientId.equals("client-b")) {
            isClientBAlive.set(true);
            isReconnectScheduled.set(false); // เคลียร์ flag
        }
    }

    private void scheduleReconnect() {
        if (isReconnectScheduled.compareAndSet(false, true)) {
            log.info("⏳ Waiting 30 seconds before reconnecting client-b...");
            executor.schedule(() -> {
                log.info("🔁 Reconnecting client-b now...");
                startClient("client-b");
            }, 30, TimeUnit.SECONDS);
        }
    }
}