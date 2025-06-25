package com.zencode.grpc;

import com.zengcode.grpc.HelloRequest;
import com.zengcode.grpc.HelloResponse;
import com.zengcode.grpc.HelloServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@Slf4j
@EnableScheduling
public class GrpcClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(GrpcClientApplication.class, args);
    }

}