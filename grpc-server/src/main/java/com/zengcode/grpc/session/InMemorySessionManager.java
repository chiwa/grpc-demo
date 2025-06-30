package com.zengcode.grpc.session;

import com.zengcode.grpc.chat.ChatMessage;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

@Component
@Slf4j
public class InMemorySessionManager implements ISessionManager {
    private final Map<String, StreamObserver<ChatMessage>> sessions = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeenMap = new ConcurrentHashMap<>();

    private static final long TIMEOUT_MILLIS = 30_000; // 30 วินาที
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();

    @PostConstruct
    public void startSessionMonitor() {
        monitor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (String clientId : sessions.keySet()) {
                long lastSeen = lastSeenMap.getOrDefault(clientId, 0L);
                if ((now - lastSeen) > TIMEOUT_MILLIS) {
                    log.warn("🧹 [Timeout] Cleaning inactive session: {}", clientId);
                    sessions.remove(clientId);
                    lastSeenMap.remove(clientId);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void addSession(String clientId, StreamObserver<ChatMessage> responseObserver) {
        sessions.put(clientId, responseObserver);
        updateLastSeen(clientId);
    }

    @Override
    public void updateLastSeen(String clientId) {
        lastSeenMap.put(clientId, System.currentTimeMillis());
    }

    @Override
    public void removeSession(String clientId) {
        sessions.remove(clientId);
        lastSeenMap.remove(clientId);
    }

    @Override
    public void broadcast(ChatMessage message) {
        sessions.forEach((id, observer) -> {
            try {
                observer.onNext(message);
            } catch (Exception e) {
                sessions.remove(id);
                lastSeenMap.remove(id);
                log.warn("❌ Broadcast failed, removing session: {}", id);
            }
        });
    }

    @Override
    public void sendTo(String clientId, ChatMessage message) {
        StreamObserver<ChatMessage> observer = getStreamObserverByClientId(clientId);
        if (observer != null) {
            updateLastSeen(clientId);
            observer.onNext(message);
        } else {
            sessions.remove(clientId);
            lastSeenMap.remove(clientId);
            log.warn("❌ No session found for clientId: {}", clientId);
        }
    }

    @Override
    public Set<String> getConnectedClientIds() {
        return sessions.keySet();
    }

    @Override
    public StreamObserver<ChatMessage> getStreamObserverByClientId(String clientId) {
        return sessions.get(clientId);
    }
}