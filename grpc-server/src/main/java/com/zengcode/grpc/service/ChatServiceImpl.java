package com.zengcode.grpc.service;

import com.zengcode.grpc.chat.ChatMessage;
import com.zengcode.grpc.chat.ChatServiceGrpc;
import com.zengcode.grpc.session.ISessionManager;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@GrpcService
@Slf4j
public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private final ISessionManager sessionManager;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicInteger counter = new AtomicInteger(1);

    public ChatServiceImpl(ISessionManager sessionManager) {
        this.sessionManager = sessionManager;

        scheduler.scheduleAtFixedRate(() -> {
            ChatMessage broadcast = ChatMessage.newBuilder()
                    .setClientId("Server")
                    .setContent("Hi, this is public message(" + counter.getAndIncrement() + ") from Server to all clients")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setIsBroadcast(true)
                    .build();

            log.info("📢 Broadcasting message to all clients: {}", broadcast.getContent());
            sessionManager.broadcast(broadcast);
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
                    sessionManager.addSession(clientId, responseObserver);
                    log.info("✅ New client connected: {}", clientId);
                }

                log.info("📥 [Server Received] {}: {}", value.getClientId(), value.getContent());

                ChatMessage reply = ChatMessage.newBuilder()
                        .setClientId("Server")
                        .setContent("Ok I got Message: " + value.getContent())
                        .setTimestamp(Instant.now().toEpochMilli())
                        .build();

                responseObserver.onNext(reply);
                log.info("📤 [Server Replied] to {}: {}", clientId, reply.getContent());
            }

            @Override
            public void onError(Throwable t) {
                log.warn("❌ [Error] Client {}: {}", clientId, t.getMessage());
                if (clientId != null) {
                    sessionManager.removeSession(clientId);
                    log.info("👋 Session removed due to error: {}", clientId);
                }
            }

            @Override
            public void onCompleted() {
                log.info("🔚 [Completed] Client disconnected: {}", clientId);
                if (clientId != null) {
                    sessionManager.removeSession(clientId);
                    log.info("🗑️ Session removed: {}", clientId);
                }
                responseObserver.onCompleted();
            }
        };
    }
}