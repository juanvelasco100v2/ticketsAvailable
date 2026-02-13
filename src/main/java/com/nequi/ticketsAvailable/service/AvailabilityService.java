package com.nequi.ticketsAvailable.service;

import com.nequi.ticketsAvailable.dto.AvailabilityDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
public class AvailabilityService {

    private static final Logger logger = LoggerFactory.getLogger(AvailabilityService.class);
    private final ReactiveRedisTemplate<String, AvailabilityDTO> redisTemplate;
    
    private final Sinks.Many<AvailabilityDTO> sink = Sinks.many().multicast().onBackpressureBuffer();
    
    private static final String CHANNEL_NAME = "events:availability";

    public AvailabilityService(ReactiveRedisTemplate<String, AvailabilityDTO> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        logger.info("Initializing Redis listener for channel: {}", CHANNEL_NAME);
        
        redisTemplate.listenToChannel(CHANNEL_NAME)
                .map(message -> message.getMessage()) // Extraer el DTO del mensaje Redis
                .doOnNext(dto -> {
                    logger.info("Received update from Redis for event {}: Available={}, Reserved={}",
                            dto.eventId(), dto.availableCapacity(), dto.reservedCount());
                    
                    Sinks.EmitResult result = sink.tryEmitNext(dto);
                    if (result.isFailure()) {
                        logger.warn("Failed to emit to sink: {}", result);
                    }
                })
                .doOnError(e -> logger.error("Error listening to Redis channel", e))
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(2))
                        .filter(e -> e instanceof RedisConnectionFailureException)
                        .doBeforeRetry(signal -> logger.warn("Retrying Redis connection... Attempt {}", signal.totalRetries() + 1)))
                .subscribe(
                        null, 
                        error -> logger.error("Fatal error in Redis listener stream after retries", error)
                );
    }

    public Flux<AvailabilityDTO> streamAvailability(String eventId) {
        logger.info("New client connected to stream for event: {}", eventId);
        return sink.asFlux()
                .filter(dto -> {
                    boolean match = dto.eventId().equals(eventId);
                    if (match) {
                        logger.info("Pushing update to client for event {}", eventId);
                    }
                    return match;
                })
                .doOnCancel(() -> logger.info("Client disconnected from stream for event {}", eventId));
    }
    
    public void publishUpdate(AvailabilityDTO update) {
        logger.info("Publishing update to Redis channel {}: {}", CHANNEL_NAME, update);
        redisTemplate.convertAndSend(CHANNEL_NAME, update)
                .doOnSuccess(count -> logger.info("Update published. Receivers: {}", count))
                .doOnError(e -> logger.error("Error publishing update to Redis", e))
                .subscribe();
    }
}