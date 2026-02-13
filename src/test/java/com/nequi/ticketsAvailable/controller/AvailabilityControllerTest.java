package com.nequi.ticketsAvailable.controller;

import com.nequi.ticketsAvailable.dto.AvailabilityDTO;
import com.nequi.ticketsAvailable.service.AvailabilityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvailabilityControllerTest {

    @Mock
    private AvailabilityService availabilityService;

    @InjectMocks
    private AvailabilityController availabilityController;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(availabilityController).build();
    }

    @Test
    void streamAvailability_ShouldReturnSseStream() {
        AvailabilityDTO dto = new AvailabilityDTO("event-1", 100, 0);
        when(availabilityService.streamAvailability("event-1")).thenReturn(Flux.just(dto));

        webTestClient.get()
                .uri("/api/v1/availability/stream/event-1")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBodyList(AvailabilityDTO.class)
                .contains(dto);
    }

    @Test
    void publishUpdate_ShouldCallService() {
        AvailabilityDTO update = new AvailabilityDTO("event-1", 90, 10);

        webTestClient.post()
                .uri("/api/v1/availability/update")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(update)
                .exchange()
                .expectStatus().isOk();

        verify(availabilityService).publishUpdate(any(AvailabilityDTO.class));
    }
}
