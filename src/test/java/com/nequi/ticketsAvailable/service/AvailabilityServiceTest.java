package com.nequi.ticketsAvailable.service;

import com.nequi.ticketsAvailable.dto.AvailabilityDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq; // Import explícito de eq
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvailabilityServiceTest {

    @Mock
    private ReactiveRedisTemplate<String, AvailabilityDTO> redisTemplate;

    private AvailabilityService availabilityService;

    @BeforeEach
    void setUp() {
        availabilityService = new AvailabilityService(redisTemplate);
    }

    @Test
    void init_ShouldSubscribeToRedisChannel() {
        ReactiveSubscription.Message<String, AvailabilityDTO> message = mock(ReactiveSubscription.Message.class);
        AvailabilityDTO dto = new AvailabilityDTO("event-1", 100, 0);
        
        when(message.getMessage()).thenReturn(dto);
        
        Flux<ReactiveSubscription.Message<String, AvailabilityDTO>> messageFlux = Flux.just(message);
        doReturn(messageFlux).when(redisTemplate).listenToChannel(anyString());

        availabilityService.init();

        StepVerifier.create(availabilityService.streamAvailability("event-1"))
                .expectNext(dto)
                .thenCancel()
                .verify();
    }

    @Test
    void streamAvailability_ShouldFilterByEventId() {
        ReactiveSubscription.Message<String, AvailabilityDTO> msg1 = mock(ReactiveSubscription.Message.class);
        when(msg1.getMessage()).thenReturn(new AvailabilityDTO("event-1", 100, 0));
        
        ReactiveSubscription.Message<String, AvailabilityDTO> msg2 = mock(ReactiveSubscription.Message.class);
        when(msg2.getMessage()).thenReturn(new AvailabilityDTO("event-2", 50, 0));

        Flux<ReactiveSubscription.Message<String, AvailabilityDTO>> messageFlux = Flux.just(msg1, msg2);
        doReturn(messageFlux).when(redisTemplate).listenToChannel(anyString());
        
        availabilityService.init();

        StepVerifier.create(availabilityService.streamAvailability("event-1"))
                .expectNextMatches(dto -> dto.eventId().equals("event-1"))
                .thenCancel()
                .verify();
    }

    @Test
    void publishUpdate_ShouldSendToRedis() {
        AvailabilityDTO update = new AvailabilityDTO("event-1", 90, 10);
        when(redisTemplate.convertAndSend(anyString(), any(AvailabilityDTO.class))).thenReturn(Mono.just(1L));

        availabilityService.publishUpdate(update);

        verify(redisTemplate).convertAndSend(anyString(), eq(update));
    }
}
