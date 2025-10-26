package com.javatechie.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.model.Transaction;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeserializer implements Deserializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String topic, byte[] bytes) {
        try {
            return mapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Transaction", e);
        }
    }
}
