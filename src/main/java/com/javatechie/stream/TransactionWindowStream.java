package com.javatechie.stream;

import com.javatechie.model.Transaction;
import com.javatechie.serdes.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Slf4j
public class TransactionWindowStream {

    @Bean
    public KStream<String, Transaction> windowedTransactionStream(StreamsBuilder builder) {

        KStream<String, Transaction> stream = null;

        try (var transactionSerde = new JsonSerde<>(Transaction.class)) {

            // Step 1: Read from input topic
            stream = builder.stream("transactions",
                    Consumed.with(
                            Serdes.String(), new TransactionSerde()
                    ));
            // Step 2: Group by userId and apply tumbling window of 10 seconds
            stream
                    .groupBy((key, tx) -> tx.userId(),
                            Grouped.with(Serdes.String(), transactionSerde))
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                    .count(Materialized.as("user-txn-count-window-store"))
                    .toStream()
                    .peek((windowedKey, count) -> {
                        String user = windowedKey.key();
                        log.info("🧾 User={} | Count={} | Window=[{} - {}]",
                                user,
                                count,
                                windowedKey.window().startTime(),
                                windowedKey.window().endTime());

                        if (count > 3) {
                            log.warn("🚨 FRAUD ALERT: User={} made {} transactions within 10 seconds!", user, count);
                        }
                    })
                    .to("user-txn-counts",
                            Produced.with(
                                    WindowedSerdes.timeWindowedSerdeFrom(String.class),
                                    Serdes.Long()
                            )
                    );
        }


        return stream;
    }
}
