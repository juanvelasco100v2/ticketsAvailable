package com.nequi.ticketsAvailable.dto;

public record AvailabilityDTO(String eventId, int availableCapacity, int reservedCount) {}