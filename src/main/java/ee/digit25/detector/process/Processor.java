package ee.digit25.detector.process;

import ee.digit25.detector.domain.transaction.TransactionValidator;
import ee.digit25.detector.domain.transaction.external.TransactionRequester;
import ee.digit25.detector.domain.transaction.external.TransactionVerifier;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class Processor {

    // Increased from 1 to process more transactions in each batch
    private final int TRANSACTION_BATCH_SIZE = 10;

    // Create a thread pool for parallel processing
    // Adjust thread count based on your CPU cores and workload
    private final ExecutorService executorService = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors())
    );

    private final TransactionRequester requester;
    private final TransactionValidator validator;
    private final TransactionVerifier verifier;

    @Scheduled(fixedDelay = 100) // Changed from 10ms to 100ms to reduce overhead
    public void process() {
        log.info("Starting to process a batch of transactions of size {}", TRANSACTION_BATCH_SIZE);

        List<Transaction> transactions = requester.getUnverified(TRANSACTION_BATCH_SIZE);

        if (transactions.isEmpty()) {
            log.debug("No transactions to process");
            return;
        }

        // Process transactions in parallel
        List<CompletableFuture<TransactionResult>> futures = transactions.stream()
                .map(transaction -> CompletableFuture.supplyAsync(() -> {
                    boolean isLegitimate = validator.isLegitimate(transaction);
                    return new TransactionResult(transaction, isLegitimate);
                }, executorService))
                .collect(Collectors.toList());

        // Wait for all validations to complete
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        // Process results when all validations complete
        allFutures.thenRun(() -> {
            List<Transaction> acceptTransactions = new ArrayList<>();
            List<Transaction> rejectTransactions = new ArrayList<>();

            futures.forEach(future -> {
                try {
                    TransactionResult result = future.get();
                    if (result.isLegitimate()) {
                        acceptTransactions.add(result.getTransaction());
                    } else {
                        rejectTransactions.add(result.getTransaction());
                    }
                } catch (Exception e) {
                    log.error("Error processing transaction", e);
                }
            });

            // Submit verification/rejection tasks in parallel
            if (!acceptTransactions.isEmpty()) {
                CompletableFuture.runAsync(() -> verifier.verify(acceptTransactions), executorService);
            }

            if (!rejectTransactions.isEmpty()) {
                CompletableFuture.runAsync(() -> verifier.reject(rejectTransactions), executorService);
            }
        }).join(); // Wait for processing to complete before ending the scheduled method
    }

    // Helper class to hold validation results
    private static class TransactionResult {
        private final Transaction transaction;
        private final boolean legitimate;

        public TransactionResult(Transaction transaction, boolean legitimate) {
            this.transaction = transaction;
            this.legitimate = legitimate;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public boolean isLegitimate() {
            return legitimate;
        }
    }
}