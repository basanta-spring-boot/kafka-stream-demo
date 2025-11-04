package com.javatechie.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.model.Transaction;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final StreamsBuilderFactoryBean factoryBean;

    public TransactionController(KafkaTemplate<String, Transaction> kafkaTemplate, StreamsBuilderFactoryBean factoryBean) {
        this.kafkaTemplate = kafkaTemplate;
        this.factoryBean = factoryBean;
    }

//    @PostMapping
//    public String sendTransaction() {
//        // loop 100 times to create and send sample transactions
//        for (int i = 0; i < 100; i++) {
//            // build a unique transaction id using current time and loop index
//            String transactionId = "txn-" + System.currentTimeMillis() + "-" + i;
//            // generate a random amount between 8000 and 11000
//            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
//            // create a Transaction record with id, user, amount and timestamp
//            Transaction txn = new Transaction(transactionId, "USER_" + i, amount, LocalDateTime.now().toString());
//            // send the Transaction object to the 'transactions' topic with the transactionId as key
//            kafkaTemplate.send("transactions", transactionId, txn);
//        }
//
//        // return a confirmation message
//        return "✅ Transaction sent to Kafka!";
//    }

    /**
     * Read `transactions.json` from the classpath and convert it to a List<Transaction>.
     * Each line below has a short comment describing what it does.
     */
    private List<Transaction> readTransactionsFromResource() {
        // open the resource stream for /transactions.json from the classpath
        try (InputStream is = getClass().getResourceAsStream("/transactions.json")) {
            return mapper.readValue(is, new TypeReference<List<Transaction>>() {});
        } catch (Exception e) {
            // wrap any parsing errors and rethrow as a runtime exception
            throw new RuntimeException("Failed to parse transactions.json", e);
        }
    }

    /**
     * Publish transactions loaded from the classpath resource to Kafka.
     * This method uses the helper above and sends each transaction to the "transactions" topic.
     */
    @PostMapping("/publish")
    public String publishTransaction() {
        // load the list of transactions from the resource file
        List<Transaction> transactions = readTransactionsFromResource();

        // iterate over each transaction in the list
        for (Transaction txn : transactions) {
            // send the transaction to Kafka using the transaction id (record accessor) as the key
            kafkaTemplate.send("transactions", txn.transactionId(), txn);
        }
        // return a summary message stating how many transactions were published
        return "✅ Published " + transactions.size() + " transactions to Kafka!";
    }

    @GetMapping("/user/{userId}/count")
    public String getTxnCount(@PathVariable String userId) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return "Kafka Streams not initialized yet!";
        }

        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType("user-txn-count-store", QueryableStoreTypes.keyValueStore())
        );

        Long count = store.get(userId);
        if (count == null) {
            return "No transactions found for user: " + userId;
        }

        return "User " + userId + " made " + count + " transactions.";
    }

    @GetMapping("/users/counts")
    public Map<String, Long> getAllCounts() {
        KafkaStreams streams = factoryBean.getKafkaStreams();

        ReadOnlyKeyValueStore<String, Long> store =
                streams.store(StoreQueryParameters
                        .fromNameAndType(
                                "user-txn-count-store",
                                QueryableStoreTypes.keyValueStore()
                        ));

        Map<String, Long> results = new HashMap<>();
        store.all().forEachRemaining(entry -> results.put(entry.key, entry.value));
        return results;
    }

}
