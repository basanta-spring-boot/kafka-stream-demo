package com.javatechie.serdes;

import com.javatechie.model.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
    public TransactionSerde() {
        super(new TransactionSerializer(), new TransactionDeserializer());
    }
}
