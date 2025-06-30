package com.zengcode.grpc.session;

import com.zengcode.grpc.chat.ChatMessage;
import io.grpc.stub.StreamObserver;

import java.util.Set;

public interface ISessionManager {
    void addSession(String clientId, StreamObserver<ChatMessage> responseObserver);
    void removeSession(String clientId);
    void broadcast(ChatMessage message);
    void sendTo(String clientId, ChatMessage message);
    Set<String> getConnectedClientIds();
    StreamObserver<ChatMessage> getStreamObserverByClientId(String clientId);
    void updateLastSeen(String clientId);
}
