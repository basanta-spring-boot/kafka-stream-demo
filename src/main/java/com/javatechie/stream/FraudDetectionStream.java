//package com.javatechie.stream;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.javatechie.model.Item;
//import com.javatechie.model.Transaction;
//import com.javatechie.serdes.TransactionSerde;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.*;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafkaStreams;
//import org.springframework.kafka.support.serializer.JsonSerde;
//
//import java.util.ArrayList;
//import java.util.List;
//
//@Configuration
//@EnableKafkaStreams
//@Slf4j
//public class FraudDetectionStream {
//
//    // Reuse a single ObjectMapper instance (thread-safe for read operations) to avoid creating one per record
//    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//
//    /**
//     * <p>
//     * Kafka Streams operations used:
//     * <p>
//     * filter(),
//     * filterNot(),
//     * map(),
//     * mapValues(),
//     * flatMap(),
//     * flatMapValues(),
//     * branch(),
//     * groupByKey(),
//     * aggregate(),
//     * count(),
//     * -> keep it for upcoming class -> join() & windowedBy()
//     *
//     */
//    @Bean
//    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {
//
//        var transactionSerde = new JsonSerde<>(Transaction.class);
//
//        KStream<String, Transaction> stream = builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));
//
//        //filter transactions greater than 10,000
//        stream.filter((key, tx) -> tx.amount() > 10000)
//                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx));
//
//        //don't process transactions less than 10,000
//        stream.filterNot((key, tx) -> tx.amount() < 10000)
//                .peek((key, tx) -> log.info("✅ high transaction {}", tx));
//
//        //map user transaction summary with new key-value pair
//        //map() lets us completely reshape our data — new key, new value.
//        // Here we’re making the userId the key and creating a human-readable message as the value.
//
//        stream.map((key, tx) ->
//                KeyValue.pair(tx.userId(), "User spent ₹" + tx.amount())
//        ).peek((key, tx) ->
//                log.info("User Transaction Summary: Key: {}, Value: {}", key, tx)
//        );
//
//        //mapValues() lets us change just the value, keeping the original key intact.
//        //Unlike map(), this one keeps the same key.
//        // It’s faster and more efficient — only the value changes.
//        // Great when you just want to enrich or format your message
//
//        stream.mapValues(tx ->
//                "Transaction of ₹" + tx.amount() + " by user " + tx.userId()
//        ).peek((key, tx) ->
//                log.info("User Transaction Summary Value Only: Key: {}, Value: {}", key, tx)
//        );
//
//        //flatMap lets us take one input record and produce multiple output records.
//        // 🧩 Use flatMap to explode a single transaction into multiple item events
//        //Each Transaction contains multiple items.
//        //But you want to emit one record per item — not per transaction.
//
//        stream.flatMap((key, tx) -> {
//            List<KeyValue<String, Item>> results = new ArrayList<>();
//            for (Item item : tx.items()) {
//                results.add(KeyValue.pair(tx.transactionId(), item));
//            }
//            return results;
//        }).peek((key, item) ->
//                log.info("flatMap -- Item Purchased: Transaction ID: {}, Item: {}", key, item)
//        );
//
//        //flatMapValues() is similar to flatMap(), but it only transforms the value.
//        stream.flatMapValues(tx-> tx.items())
//                .peek((key, item) ->
//                        log.info("flatMapValues -- Item Purchased Value Only: Transaction ID: {}, Item: {}", key, item)
//                );
//
//        //⚙️ 1. branch() – Split one stream into multiple
//        //💬 Use Case: Separate “credit” and “debit” transactions.
//
//        KStream<String, Transaction>[] branches = stream.branch(
//                (key, tx) -> tx.type().equalsIgnoreCase("debit"),
//                (key, tx) -> tx.type().equalsIgnoreCase("credit")
//        );
//
//        //Perfect for routing data to different Kafka topics.
//
//        branches[0].peek((key, tx) ->
//                log.info("Debit Transaction: Key: {}, Transaction: {}", key, tx)
//        ).to("debit_transactions", Produced.with(Serdes.String(), transactionSerde));
//
//        branches[1].peek((key, tx) ->
//                log.info("Credit Transaction: Key: {}, Transaction: {}", key, tx)
//        ).to("credit_transactions", Produced.with(Serdes.String(), transactionSerde));
//
//        //👥 3. groupByKey() – Group records that share the same key
//        //💬 Use Case: Group transactions by location for further aggregation.
//        stream
//                .groupBy((key, tx) -> tx.location())
//                .count()
//                .toStream()
//                .peek((location, count) -> log.info("🌍 Location {} has {} transactions", location, count));
//
//
//        //💬 Use Case: Count the number of transactions per user. (groupBy + count)
//        stream
//                .groupBy((key, tx) -> tx.userId())
//                .count(Materialized.as("user-txn-count-store"))  // ✅ store name
//                .toStream()
//                .peek((userId, count) -> log.info("👥 User {} made {} transactions", userId, count));
//
//
//        //➕ 4. aggregate() – Perform custom aggregations
//        //💬 Use Case: Calculate the running total amount spent per transaction Type.
//        stream
//                .groupBy((key, tx) -> tx.type())
//                .aggregate(
//                        () -> 0.0, // Initializer (starting value)
//                        (location, tx, currentSum) -> currentSum + tx.amount(), // Adder
//                        Materialized.with(Serdes.String(), Serdes.Double()) // State store types
//                )
//                .toStream()
//                .peek((type, total) -> log.info("🌍 CardType: {} | 💰 Running Total Amount: {}", type, total));
//
//        return stream;
//
//
//    }
//
//
//}
