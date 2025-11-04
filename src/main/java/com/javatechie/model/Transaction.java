package com.javatechie.model;

import java.util.List;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String location,
        String type,
        List<Item> items
) {
}

