package com.x.notification.realtime;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

import java.util.Map;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class RealtimeConfig {

    @Bean
    public HandlerMapping realtimeMapping(RealtimeWebSocketHandler handler, @Value("${x.realtime.path}") String path) {
        return new SimpleUrlHandlerMapping(Map.of(path, handler), -1);
    }

    @Bean
    public NewTopic realtimeTopic(@Value("${x.realtime.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    /** For operators inside the cluster; the gateway only routes /ws/**. */
    @Bean
    public RouterFunction<ServerResponse> realtimeStatus(ConnectionRegistry registry) {
        return route().GET("/internal/realtime/status", request -> ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("connections", registry.size()))).build();
    }
}
