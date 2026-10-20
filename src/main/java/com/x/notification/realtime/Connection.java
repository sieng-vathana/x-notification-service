package com.x.notification.realtime;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/** One open browser connection and the messages waiting to be written to it. */
public final class Connection {

    private final Viewer viewer;
    private final Sinks.Many<String> outbox = Sinks.many().unicast().onBackpressureBuffer();

    Connection(Viewer viewer) {
        this.viewer = viewer;
    }

    public Viewer viewer() {
        return viewer;
    }

    Flux<String> messages() {
        return outbox.asFlux();
    }

    /** Events arrive on the Kafka thread, so writes from several threads are serialised here. */
    void send(String message) {
        outbox.emitNext(message, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(200)));
    }

    void close() {
        outbox.tryEmitComplete();
    }
}
