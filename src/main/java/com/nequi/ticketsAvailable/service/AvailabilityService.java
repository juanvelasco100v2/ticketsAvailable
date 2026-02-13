package com.nequi.ticketsAvailable.service;

import com.nequi.ticketsAvailable.dto.AvailabilityDTO;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

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

    @PostConstruct
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
                .subscribe();
    }

    public Flux<AvailabilityDTO> streamAvailability(String eventId) {
        return sink.asFlux()
                .filter(dto -> dto.eventId().equals(eventId))
                .doOnCancel(() -> logger.debug("Client disconnected from stream for event {}", eventId));
    }
    
    public void publishUpdate(AvailabilityDTO update) {
        // Publicar en el canal de Redis para que todas las instancias (si hubiera réplicas) reciban la actualización
        redisTemplate.convertAndSend(CHANNEL_NAME, update).subscribe();
    }
}