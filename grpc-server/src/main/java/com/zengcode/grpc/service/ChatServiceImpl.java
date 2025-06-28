package com.zengcode.grpc.service;

import com.zengcode.grpc.chat.ChatMessage;
import com.zengcode.grpc.chat.ChatServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@GrpcService
@Slf4j
public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private final Map<String, StreamObserver<ChatMessage>> connectedClients = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    AtomicInteger counter = new AtomicInteger(1);
    public ChatServiceImpl() {
        scheduler.scheduleAtFixedRate(() -> {
            ChatMessage broadcast = ChatMessage.newBuilder()
                    .setClientId("Server")
                    .setContent("Hi, this is public message(" + counter.getAndIncrement() + ") from Server to all clients")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setIsBroadcast(true)
                    .build();
            broadcastToAllClients(broadcast);
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {
        return new StreamObserver<>() {
            private String clientId;

            @Override
            public void onNext(ChatMessage value) {
                if (clientId == null) {
                    clientId = value.getClientId();
                    connectedClients.put(clientId, responseObserver);
                }

                log.info("[Server Received] {}: {}", value.getClientId(), value.getContent());

                ChatMessage reply = ChatMessage.newBuilder()
                        .setClientId("Server")
                        .setContent("Ok I got Message: " + value.getContent())
                        .setTimestamp(Instant.now().toEpochMilli())
                        .build();

                responseObserver.onNext(reply);
            }

            @Override
            public void onError(Throwable t) {
                if (clientId != null) connectedClients.remove(clientId);
            }

            @Override
            public void onCompleted() {
                if (clientId != null) connectedClients.remove(clientId);
                responseObserver.onCompleted();
            }
        };
    }

    private void broadcastToAllClients(ChatMessage message) {
        connectedClients.forEach((clientId, client) -> {
            try {
                client.onNext(message);
            } catch (Exception e) {
                connectedClients.remove(clientId);
            }
        });
    }
}
