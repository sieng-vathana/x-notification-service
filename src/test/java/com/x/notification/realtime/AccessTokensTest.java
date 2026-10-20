package com.x.notification.realtime;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AccessTokensTest {

    private static final String SECRET = "test-secret-that-is-at-least-32-characters-long";
    private final AccessTokens tokens = new AccessTokens(SECRET);

    @Test
    void anAccessTokenNamesTheUserAndBusiness() {
        Viewer viewer = tokens.read(sign(SECRET, "access", 60_000)).orElseThrow();
        assertThat(viewer.userId()).isEqualTo(7L);
        assertThat(viewer.businessId()).isEqualTo(2L);
        assertThat(viewer.admin()).isFalse();
    }

    @Test
    void refreshExpiredAndForeignTokensAreRefused() {
        assertThat(tokens.read(sign(SECRET, "refresh", 60_000))).isEmpty();
        assertThat(tokens.read(sign(SECRET, "access", -1_000))).isEmpty();
        assertThat(tokens.read(sign("another-secret-that-is-at-least-32-characters", "access", 60_000))).isEmpty();
        assertThat(tokens.read(null)).isEmpty();
    }

    @Test
    void eventsReachTheirBusinessUserOrStaff() {
        Viewer merchant = tokens.read(sign(SECRET, "access", 60_000)).orElseThrow();
        assertThat(merchant.receives(new RealtimeEvent("payment.updated", 2L, null, false, null))).isTrue();
        assertThat(merchant.receives(new RealtimeEvent("payment.updated", 3L, null, false, null))).isFalse();
        assertThat(merchant.receives(new RealtimeEvent("telegram.mfa.linked", null, 7L, false, null))).isTrue();
        assertThat(merchant.receives(new RealtimeEvent("approvals.changed", null, null, true, null))).isFalse();
    }

    private static String sign(String secret, String type, long lifetimeMillis) {
        return Jwts.builder()
                .setSubject("sota")
                .claim("tokenType", type)
                .claim("uid", 7)
                .claim("bid", 2)
                .claim("permissions", List.of("x-order:read"))
                .setExpiration(new Date(System.currentTimeMillis() + lifetimeMillis))
                .signWith(Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8)), SignatureAlgorithm.HS256)
                .compact();
    }
}
