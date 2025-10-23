package com.javatechie.model;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {
}

