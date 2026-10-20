package com.x.notification.realtime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * An event another service put on the x.realtime topic. It goes to everyone signed in to businessId, to the one
 * user userId, and, when admins is true, to backoffice staff; data is passed to the browser as it is.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RealtimeEvent(String type, Long businessId, Long userId, boolean admins, JsonNode data) {
}
