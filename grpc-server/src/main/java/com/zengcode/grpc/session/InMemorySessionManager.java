package com.zengcode.grpc.session;

import com.zengcode.grpc.chat.ChatMessage;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class InMemorySessionManager implements ISessionManager {
    private final Map<String, StreamObserver<ChatMessage>> sessions = new ConcurrentHashMap<>();

    @Override
    public void addSession(String clientId, StreamObserver<ChatMessage> responseObserver) {
        sessions.put(clientId, responseObserver);
    }

    @Override
    public void removeSession(String clientId) {
        sessions.remove(clientId);
    }

    @Override
    public void broadcast(ChatMessage message) {
        sessions.forEach((id, observer) -> {
            try {
                log.info("Broadcast message to client : {}", id);
                observer.onNext(message);
            } catch (Exception e) {
                sessions.remove(id);
                log.warn("❌ No session found for clientId: {}", id);
            }
        });
    }

    @Override
    public void sendTo(String clientId, ChatMessage message) {
        StreamObserver<ChatMessage> observer = getStreamObserverByClientId(clientId);
        if (observer != null) {
            observer.onNext(message);
        } else {
            sessions.remove(clientId);
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
