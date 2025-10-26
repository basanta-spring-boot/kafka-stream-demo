package com.javatechie.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.model.Transaction;
import com.javatechie.serdes.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStream {

    // Reuse a single ObjectMapper instance (thread-safe for read operations) to avoid creating one per record
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Builds the Kafka Streams topology for fraud detection.
     * Flow:
     * 1. Read messages from the "transactions" topic as a KStream.
     * 2. Apply a filter that retains only transactions considered fraudulent.
     * 3. Log a fraud alert (with transaction id, key and amount when available).
     * 4. Write fraudulent transactions to the "fraud-alerts" topic.
     * 5. Return the original transaction stream (useful for testing or further processing).
     */
    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

        var transactionSerde = new JsonSerde<>(Transaction.class);

        //approach 1
//        KStream<String, Transaction> stream = builder
//                .stream("transactions", Consumed.with(Serdes.String(), transactionSerde));
//
//        stream.filter((key, tx) -> tx.amount() > 10000)
//                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx))
//                .to("fraud-alerts", Produced.with(Serdes.String(), transactionSerde));

        //approach 2
        KStream<String, Transaction> stream = builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        stream.filter((key, tx) -> tx.amount() > 10000)
                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx))
                .to("fraud-alerts", Produced.with(Serdes.String(), transactionSerde));

        return stream;
    }


}
