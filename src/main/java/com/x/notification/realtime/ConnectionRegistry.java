package com.x.notification.realtime;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The browser connections open on this instance. */
@Component
public class ConnectionRegistry {

    private final Set<Connection> connections = ConcurrentHashMap.newKeySet();

    public Connection open(Viewer viewer) {
        Connection connection = new Connection(viewer);
        connections.add(connection);
        return connection;
    }

    public void close(Connection connection) {
        if (connections.remove(connection)) {
            connection.close();
        }
    }

    /** Writes the message to every connection the event is meant for; returns how many got it. */
    public int deliver(RealtimeEvent event, String message) {
        int delivered = 0;
        for (Connection connection : connections) {
            if (connection.viewer().receives(event)) {
                connection.send(message);
                delivered++;
            }
        }
        return delivered;
    }

    public int size() {
        return connections.size();
    }
}
