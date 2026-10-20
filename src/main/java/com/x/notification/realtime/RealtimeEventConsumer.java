package com.x.notification.realtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Takes events off the x.realtime topic and pushes them to the connections they are meant for. */
@Component
public class RealtimeEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RealtimeEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final ConnectionRegistry registry;

    public RealtimeEventConsumer(ObjectMapper objectMapper, ConnectionRegistry registry) {
        this.objectMapper = objectMapper;
        this.registry = registry;
    }

    @KafkaListener(topics = "${x.realtime.topic}")
    public void onEvent(String json) {
        try {
            RealtimeEvent event = objectMapper.readValue(json, RealtimeEvent.class);
            if (event.type() == null || event.type().isBlank()) {
                return;
            }
            ObjectNode message = objectMapper.createObjectNode().put("type", event.type());
            message.set("data", event.data() == null ? objectMapper.createObjectNode() : event.data());
            int delivered = registry.deliver(event, objectMapper.writeValueAsString(message));
            log.info("Realtime {} delivered to {} connection(s)", event.type(), delivered);
        } catch (JsonProcessingException exception) {
            log.warn("Unreadable realtime event skipped: {}", exception.getOriginalMessage());
        }
    }
}
