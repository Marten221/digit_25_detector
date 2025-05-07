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
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class Processor {

    // Increased batch size for better throughput
    private final int TRANSACTION_BATCH_SIZE = 24;

    // Create a thread pool for parallel processing with proper naming
    private final ExecutorService executorService = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "transaction-processor");
                t.setDaemon(true); // Allow JVM to exit if tasks are still running during shutdown
                return t;
            }
    );

    private final TransactionRequester requester;
    private final TransactionValidator validator;
    private final TransactionVerifier verifier;

    // Collector queues for bulk operations
    private final ConcurrentLinkedQueue<String> toVerify = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> toReject = new ConcurrentLinkedQueue<>();

    @Scheduled(fixedDelay = 250) // Increased delay to reduce system load
    public void process() {
        List<Transaction> transactions = requester.getUnverified(TRANSACTION_BATCH_SIZE);

        if (transactions.isEmpty()) {
            log.debug("No transactions to process");
            return;
        }

        log.info("Processing batch of {} transactions", transactions.size());

        // Process transactions in parallel and collect futures
        List<CompletableFuture<Void>> futures = transactions.stream()
                .map(transaction -> CompletableFuture
                        .supplyAsync(() -> validator.isLegitimate(transaction), executorService)
                        .thenAcceptAsync(isLegitimate -> {
                            String transactionId = transaction.getId();
                            try {
                                if (isLegitimate) {
                                    // Queue for batch verification instead of immediate API call
                                    verifier.verify(transactionId);
                                    //log.debug("Transaction {} queued for verification", transactionId);
                                } else {
                                    // Queue for batch rejection instead of immediate API call
                                    verifier.reject(transactionId);
                                    //log.debug("Transaction {} queued for rejection", transactionId);
                                }
                            } catch (Exception e) {
                                log.error("Failed to process transaction {}: {}", transactionId, e.getMessage(), e);
                            }
                        }, executorService))
                .toList();

        // Wait for all processing to complete before ending the scheduled method
        // Using exceptionally to ensure we don't silently fail
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .exceptionally(throwable -> {
                    log.error("Batch processing error: {}", throwable.getMessage(), throwable);
                    return null;
                })
                .join();

        log.info("Completed processing batch of {} transactions", transactions.size());
    }

    // Add a shutdown hook to clean up resources
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down transaction processor");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in the specified time.");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
}