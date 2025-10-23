package com.javatechie.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

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
    public KStream<String, String> fraudDetectStream(StreamsBuilder builder) {

        // Step 1: Read messages from the input topic.
        // The stream key and value are both strings in this example.
        KStream<String, String> transactionStream = builder
                .stream("transactions");

        // Step 2: Process the stream to detect fraudulent transactions.
        // The filter uses a predicate that checks the transaction payload.
        // Step 3: Use peek() to emit alert logs for every detected fraud before sending to the output topic.
        KStream<String, String> fraudStream = transactionStream
                .filter((key, value) -> isFraudulent(value))
                .peek((key, value) ->
                        log.warn("FRAUD ALERT - transactionId={} , value={}", key,  value));

        // Step 4: Emit detected fraudulent transactions to an output topic.
        // Downstream consumers can subscribe to "fraud-alerts".
        fraudStream.to("fraud-alerts");

        // Step 5: Return the original transaction stream.
        // Returning the original stream allows further topology wiring or testing.
        return transactionStream;

    }



    private boolean isFraudulent(String value) {
        try {
            Transaction transaction=OBJECT_MAPPER
                    .readValue(value, Transaction.class); // validate JSON
            return transaction.amount() > 10000; // simple fraud rule
        } catch (Exception e) {
            return false;
        }
    }
}
