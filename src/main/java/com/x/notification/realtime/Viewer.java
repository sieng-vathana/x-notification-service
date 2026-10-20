package com.x.notification.realtime;

import java.time.Instant;

/** Who is behind a WebSocket connection, read from their access token. */
public record Viewer(Long userId, Long businessId, boolean admin, Instant expiresAt) {

    public boolean receives(RealtimeEvent event) {
        return (event.businessId() != null && event.businessId().equals(businessId))
                || (event.userId() != null && event.userId().equals(userId))
                || (event.admins() && admin);
    }

    public boolean expired(Instant now) {
        return !expiresAt.isAfter(now);
    }
}
