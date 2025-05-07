package ee.digit25.detector.domain.transaction.external;

import ee.bitweb.core.retrofit.RetrofitRequestExecutor;
import ee.digit25.detector.domain.transaction.external.api.TransactionApiProperties;
import ee.digit25.detector.domain.transaction.external.api.TransactionsApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionVerifier {

    private final TransactionsApi api;
    private final TransactionApiProperties properties;

    // Queue for batching verification requests
    private final ConcurrentLinkedQueue<String> verifyQueue = new ConcurrentLinkedQueue<>();

    // Queue for batching rejection requests
    private final ConcurrentLinkedQueue<String> rejectQueue = new ConcurrentLinkedQueue<>();

    // Counters for monitoring queue sizes
    private final AtomicInteger verifyCount = new AtomicInteger(0);
    private final AtomicInteger rejectCount = new AtomicInteger(0);

    // Configuration for batch processing
    private static final int BATCH_SIZE = 50;
    private static final int BATCH_INTERVAL_MS = 200;

    // Executor for running batch operations
    private final ExecutorService batchExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "transaction-verifier");
        t.setDaemon(true);
        return t;
    });

    // Scheduler for periodic batch processing
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "transaction-verifier-scheduler");
        t.setDaemon(true);
        return t;
    });

    // Initialize batch processing on service startup
    {
        scheduler.scheduleWithFixedDelay(
                this::processBatches,
                BATCH_INTERVAL_MS,
                BATCH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        // Add shutdown hook for clean termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down transaction verifier");
            scheduler.shutdown();
            processBatches(); // Process any remaining items
            batchExecutor.shutdown();
            try {
                if (!batchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in the specified time.");
                    batchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                batchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }

    /**
     * Queue a single transaction for verification
     */
    public void verify(String id) {
        verifyQueue.add(id);
        int size = verifyCount.incrementAndGet();

        // If queue reaches batch size, trigger immediate processing
        if (size >= BATCH_SIZE) {
            CompletableFuture.runAsync(this::processBatches, batchExecutor);
        }
    }

    /**
     * Queue a single transaction for rejection
     */
    public void reject(String id) {
        rejectQueue.add(id);
        int size = rejectCount.incrementAndGet();

        // If queue reaches batch size, trigger immediate processing
        if (size >= BATCH_SIZE) {
            CompletableFuture.runAsync(this::processBatches, batchExecutor);
        }
    }

    /**
     * Queue multiple transactions for verification
     */
    public void verify(List<String> transactions) {
        if (transactions.isEmpty()) {
            return;
        }

        log.debug("Queueing {} transactions for verification", transactions.size());
        transactions.forEach(this::verify);
    }

    /**
     * Queue multiple transactions for rejection
     */
    public void reject(List<String> transactions) {
        if (transactions.isEmpty()) {
            return;
        }

        log.debug("Queueing {} transactions for rejection", transactions.size());
        transactions.forEach(this::reject);
    }

    /**
     * Process any pending batches in both queues
     */
    private void processBatches() {
        processBatch(verifyQueue, verifyCount, this::processBatchVerify);
        processBatch(rejectQueue, rejectCount, this::processBatchReject);
    }

    /**
     * Generic batch processing method
     */
    private void processBatch(ConcurrentLinkedQueue<String> queue, AtomicInteger counter,
                              BatchProcessor processor) {
        int size = counter.get();
        if (size == 0) {
            return;
        }

        List<String> batch = new ArrayList<>(Math.min(BATCH_SIZE, size));
        String item;

        // Drain queue up to batch size
        while (batch.size() < BATCH_SIZE && (item = queue.poll()) != null) {
            batch.add(item);
            counter.decrementAndGet();
        }

        if (!batch.isEmpty()) {
            try {
                processor.process(batch);
            } catch (Exception e) {
                log.error("Failed to process batch: {}", e.getMessage(), e);
                // In case of failure, requeue the items
                requeueFailedItems(batch, queue, counter);
            }
        }
    }

    /**
     * Process a batch of verifications
     */
    private void processBatchVerify(List<String> batch) {
        if (batch.isEmpty()) return;

        log.info("Verifying batch of {} transactions", batch.size());
        RetrofitRequestExecutor.executeRaw(api.verify(properties.getToken(), batch));
        log.debug("Successfully verified {} transactions", batch.size());
    }

    /**
     * Process a batch of rejections
     */
    private void processBatchReject(List<String> batch) {
        if (batch.isEmpty()) return;

        log.info("Rejecting batch of {} transactions", batch.size());
        RetrofitRequestExecutor.executeRaw(api.reject(properties.getToken(), batch));
        log.debug("Successfully rejected {} transactions", batch.size());
    }

    /**
     * Requeue items that failed to process
     */
    private void requeueFailedItems(List<String> batch, ConcurrentLinkedQueue<String> queue,
                                    AtomicInteger counter) {
        for (String item : batch) {
            queue.add(item);
            counter.incrementAndGet();
        }
    }

    /**
     * Functional interface for batch processing
     */
    @FunctionalInterface
    private interface BatchProcessor {
        void process(List<String> batch);
    }
}