package com.x.notification.realtime;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * The browser connects with the access_token cookie it already has (or an Authorization header), gets
 * {"type":"ready"}, then every event meant for it. A ping every few seconds keeps proxies from closing a quiet
 * connection. When the token expires the connection is closed with 4401 so the browser refreshes and reconnects.
 */
@Component
public class RealtimeWebSocketHandler implements WebSocketHandler {

    static final CloseStatus SIGN_IN_AGAIN = new CloseStatus(4401, "Sign in again");
    static final String READY = "{\"type\":\"ready\"}";
    static final String PING = "{\"type\":\"ping\"}";

    private final AccessTokens accessTokens;
    private final ConnectionRegistry registry;
    private final Duration pingEvery;

    public RealtimeWebSocketHandler(AccessTokens accessTokens, ConnectionRegistry registry,
                                    @Value("${x.realtime.ping-seconds:25}") long pingSeconds) {
        this.accessTokens = accessTokens;
        this.registry = registry;
        this.pingEvery = Duration.ofSeconds(pingSeconds);
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        Optional<Viewer> viewer = accessTokens.read(token(session.getHandshakeInfo()));
        if (viewer.isEmpty() || viewer.get().expired(Instant.now())) {
            return session.close(SIGN_IN_AGAIN);
        }
        Connection connection = registry.open(viewer.get());
        Flux<String> outgoing = Flux.concat(Flux.just(READY),
                Flux.merge(connection.messages(), Flux.interval(pingEvery).map(tick -> PING)));
        Mono<Void> send = session.send(outgoing.map(session::textMessage));
        // The browser sends nothing; reading tells us when it goes away.
        Mono<Void> receive = session.receive().then();
        Mono<Void> expiry = Mono.delay(Duration.between(Instant.now(), viewer.get().expiresAt()))
                .then(session.close(SIGN_IN_AGAIN));
        return Mono.firstWithSignal(send, receive, expiry).doFinally(signal -> registry.close(connection));
    }

    static String token(HandshakeInfo handshake) {
        String authorization = handshake.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring(7);
        }
        HttpCookie cookie = handshake.getCookies().getFirst("access_token");
        return cookie == null ? null : cookie.getValue();
    }
}
