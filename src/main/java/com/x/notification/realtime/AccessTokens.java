package com.x.notification.realtime;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Collection;
import java.util.Optional;

/** Reads the access tokens the BFF issues: uid is the user, bid the business, platform:admin marks backoffice staff. */
@Component
public class AccessTokens {

    static final String ADMIN_PERMISSION = "platform:admin";

    private final Key key;

    public AccessTokens(@Value("${jwt.secret}") String secret) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    public Optional<Viewer> read(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            Claims claims = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(token.trim()).getBody();
            if (!"access".equals(claims.get("tokenType", String.class)) || claims.getExpiration() == null) {
                return Optional.empty();
            }
            Long userId = asLong(claims.get("uid"));
            Long businessId = asLong(claims.get("bid"));
            boolean admin = claims.get("permissions") instanceof Collection<?> permissions
                    && permissions.contains(ADMIN_PERMISSION);
            if (userId == null && businessId == null && !admin) {
                return Optional.empty();
            }
            return Optional.of(new Viewer(userId, businessId, admin, claims.getExpiration().toInstant()));
        } catch (JwtException | IllegalArgumentException exception) {
            return Optional.empty();
        }
    }

    private static Long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : null;
    }
}
