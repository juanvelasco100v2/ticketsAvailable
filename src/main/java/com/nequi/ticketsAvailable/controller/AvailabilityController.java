package com.nequi.ticketsAvailable.controller;

import com.nequi.ticketsAvailable.dto.AvailabilityDTO;
import com.nequi.ticketsAvailable.service.AvailabilityService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/availability")
public class AvailabilityController {

    private final AvailabilityService availabilityService;

    public AvailabilityController(AvailabilityService availabilityService) {
        this.availabilityService = availabilityService;
    }

    @GetMapping(value = "/stream/{eventId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<AvailabilityDTO> streamAvailability(@PathVariable String eventId) {
        return availabilityService.streamAvailability(eventId);
    }

    // Endpoint auxiliar para simular actualizaciones (en producción esto vendría de DynamoDB Streams -> Lambda -> Redis)
    @PostMapping("/update")
    public Mono<Void> publishUpdate(@RequestBody AvailabilityDTO update) {
        availabilityService.publishUpdate(update);
        return Mono.empty();
    }
}