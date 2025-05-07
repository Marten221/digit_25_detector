package ee.digit25.detector.domain.transaction;

import ee.digit25.detector.domain.account.AccountValidator;
import ee.digit25.detector.domain.device.DeviceValidator;
import ee.digit25.detector.domain.person.PersonValidator;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionValidator {

    private static final long VALIDATION_TIMEOUT_SECONDS = 6;

    private final PersonValidator personValidator;
    private final DeviceValidator deviceValidator;
    private final AccountValidator accountValidator;

    /**
     * Validates a transaction by checking all aspects concurrently with optimized
     * parallel execution and proper timeout handling
     */
    public boolean isLegitimate(Transaction transaction) {
        try {
            String transactionId = transaction.getId();
            log.debug("Starting validation for transaction {}", transactionId);

            String recipient = transaction.getRecipient();
            String sender = transaction.getSender();

            // Create CompletableFutures for independent validation tasks
            CompletableFuture<Boolean> recipientValidFuture = CompletableFuture.supplyAsync(
                    () -> personValidator.isValid(recipient));

            CompletableFuture<Boolean> senderValidFuture = CompletableFuture.supplyAsync(
                    () -> personValidator.isValid(sender));

            CompletableFuture<Boolean> deviceValidFuture = CompletableFuture.supplyAsync(
                    () -> deviceValidator.isValid(transaction.getDeviceMac()));

            CompletableFuture<Boolean> senderAccountValidFuture = CompletableFuture.supplyAsync(
                    () -> accountValidator.isValidSenderAccount(
                            transaction.getSenderAccount(), transaction.getAmount(), sender));

            CompletableFuture<Boolean> recipientAccountValidFuture = CompletableFuture.supplyAsync(
                    () -> accountValidator.isValidRecipientAccount(
                            transaction.getRecipientAccount(), recipient));

            // Fast-fail approach with proper timeout handling
            if (!recipientValidFuture.get(VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("Transaction {} validation failed: invalid recipient", transactionId);
                return false;
            }

            if (!senderValidFuture.get(VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("Transaction {} validation failed: invalid sender", transactionId);
                return false;
            }

            if (!deviceValidFuture.get(VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("Transaction {} validation failed: invalid device", transactionId);
                return false;
            }

            if (!senderAccountValidFuture.get(VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("Transaction {} validation failed: invalid sender account", transactionId);
                return false;
            }

            boolean isValid = recipientAccountValidFuture.get(VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!isValid) {
                log.debug("Transaction {} validation failed: invalid recipient account", transactionId);
            } else {
                log.debug("Transaction {} validation passed", transactionId);
            }

            return isValid;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            log.error("Validation interrupted", e);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.error("Error during validation: {}", e.getMessage(), e);
            return false;
        }
    }
}