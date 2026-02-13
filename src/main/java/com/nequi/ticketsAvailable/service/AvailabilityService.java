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
    
    // Sink para multidifusión (Broadcasting)
    private final Sinks.Many<AvailabilityDTO> sink = Sinks.many().multicast().onBackpressureBuffer();
    
    private static final String CHANNEL_NAME = "events:availability";

    public AvailabilityService(ReactiveRedisTemplate<String, AvailabilityDTO> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // Cambiado de @PostConstruct a @EventListener(ApplicationReadyEvent.class)
    // Esto asegura que la app ya arrancó antes de intentar conectar, evitando fallos en el inicio del contexto.
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        logger.info("Initializing Redis listener for channel: {}", CHANNEL_NAME);
        
        // Escuchar el canal de Redis y emitir al Sink
        redisTemplate.listenToChannel(CHANNEL_NAME)
                .map(message -> message.getMessage()) // Extraer el DTO del mensaje Redis
                .doOnNext(dto -> {
                    logger.debug("Received update for event {}: Available={}, Reserved={}", 
                            dto.eventId(), dto.availableCapacity(), dto.reservedCount());
                    sink.tryEmitNext(dto);
                })
                .doOnError(e -> logger.error("Error listening to Redis channel", e))
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(2)) // Más reintentos
                        .filter(e -> e instanceof RedisConnectionFailureException)
                        .doBeforeRetry(signal -> logger.warn("Retrying Redis connection... Attempt {}", signal.totalRetries() + 1)))
                .subscribe(
                        null, // onNext ya manejado en doOnNext
                        error -> logger.error("Fatal error in Redis listener stream after retries", error)
                );
    }

    public Flux<AvailabilityDTO> streamAvailability(String eventId) {
        return sink.asFlux()
                .filter(dto -> dto.eventId().equals(eventId))
                .doOnCancel(() -> logger.debug("Client disconnected from stream for event {}", eventId));
    }
    
    public void publishUpdate(AvailabilityDTO update) {
        // Publicar en el canal de Redis para que todas las instancias (si hubiera réplicas) reciban la actualización
        redisTemplate.convertAndSend(CHANNEL_NAME, update)
                .doOnError(e -> logger.error("Error publishing update to Redis", e))
                .subscribe();
    }
}