package com.x.notification.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "jwt.secret=test-secret-that-is-at-least-32-characters-long",
})
class RealtimeWebSocketTest {

    private static final String SECRET = "test-secret-that-is-at-least-32-characters-long";

    @LocalServerPort
    int port;

    @Autowired
    RealtimeEventConsumer consumer;

    @Autowired
    ConnectionRegistry registry;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void aMerchantGetsOnlyTheirOwnBusinessEvents() throws Exception {
        List<String> sota = new CopyOnWriteArrayList<>();
        List<String> other = new CopyOnWriteArrayList<>();
        Sinks.Empty<Void> done = Sinks.empty();
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        client.execute(uri(), cookie(token(7L, 2L, List.of("x-order:read"))), session -> session.receive()
                .map(WebSocketMessage::getPayloadAsText).doOnNext(sota::add).takeUntilOther(done.asMono()).then()).subscribe();
        client.execute(uri(), cookie(token(8L, 3L, List.of("x-order:read"))), session -> session.receive()
                .map(WebSocketMessage::getPayloadAsText).doOnNext(other::add).takeUntilOther(done.asMono()).then()).subscribe();
        await().atMost(Duration.ofSeconds(10)).until(() -> sota.contains(RealtimeWebSocketHandler.READY)
                && other.contains(RealtimeWebSocketHandler.READY));

        consumer.onEvent(objectMapper.writeValueAsString(Map.of(
                "type", "payment.updated", "businessId", 2, "data", Map.of("paymentId", 41, "status", "PAID"))));

        await().atMost(Duration.ofSeconds(5)).until(() -> sota.size() == 2);
        assertThat(objectMapper.readTree(sota.get(1)).path("type").asText()).isEqualTo("payment.updated");
        assertThat(objectMapper.readTree(sota.get(1)).path("data").path("paymentId").asInt()).isEqualTo(41);
        assertThat(other).containsExactly(RealtimeWebSocketHandler.READY);
        done.tryEmitEmpty();
    }

    @Test
    void withoutAValidTokenTheConnectionIsClosedWith4401() {
        AtomicReference<CloseStatus> closed = new AtomicReference<>();
        new ReactorNettyWebSocketClient().execute(uri(), cookie("not-a-token"),
                session -> session.closeStatus().doOnNext(closed::set).then()).block(Duration.ofSeconds(10));
        assertThat(closed.get().getCode()).isEqualTo(4401);
    }

    @Test
    void backofficeStaffGetAdminEvents() {
        List<String> admin = new CopyOnWriteArrayList<>();
        Sinks.Empty<Void> done = Sinks.empty();
        new ReactorNettyWebSocketClient().execute(uri(), cookie(token(1L, null, List.of("platform:admin"))), session -> session.receive()
                .map(WebSocketMessage::getPayloadAsText).doOnNext(admin::add).takeUntilOther(done.asMono()).then()).subscribe();
        await().atMost(Duration.ofSeconds(10)).until(() -> admin.contains(RealtimeWebSocketHandler.READY));

        consumer.onEvent("{\"type\":\"approvals.changed\",\"admins\":true}");

        await().atMost(Duration.ofSeconds(5)).until(() -> admin.contains("{\"type\":\"approvals.changed\",\"data\":{}}"));
        done.tryEmitEmpty();
    }

    private URI uri() {
        return URI.create("ws://localhost:" + port + "/ws/realtime");
    }

    private static HttpHeaders cookie(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, "access_token=" + token);
        return headers;
    }

    private static String token(Long userId, Long businessId, List<String> permissions) {
        var builder = Jwts.builder()
                .setSubject("user" + userId)
                .claim("tokenType", "access")
                .claim("uid", userId)
                .claim("permissions", permissions)
                .setExpiration(new Date(System.currentTimeMillis() + 60_000));
        if (businessId != null) builder.claim("bid", businessId);
        return builder.signWith(Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8)), SignatureAlgorithm.HS256).compact();
    }
}
