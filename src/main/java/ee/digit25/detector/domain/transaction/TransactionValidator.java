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

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionValidator {

    private final PersonValidator personValidator;
    private final DeviceValidator deviceValidator;
    private final AccountValidator accountValidator;

    /**
     * Validates a transaction by checking all aspects concurrently
     */
    public boolean isLegitimate(Transaction transaction) {
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

        try {
            // Fast-fail approach: if any validation returns false, we can stop
            // This short-circuits validation when any check fails
            if (!recipientValidFuture.get()) return false;
            if (!senderValidFuture.get()) return false;
            if (!deviceValidFuture.get()) return false;
            if (!senderAccountValidFuture.get()) return false;
            return recipientAccountValidFuture.get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during parallel validation", e);
            // Fail safely on exception
            return false;
        }
    }

    /**
     * Alternative implementation that uses CompletableFuture.allOf
     * This runs all validations in parallel without short-circuiting
     */
    public boolean isLegitimateAllParallel(Transaction transaction) {
        String recipient = transaction.getRecipient();
        String sender = transaction.getSender();

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

        try {
            // Wait for all futures to complete
            CompletableFuture.allOf(
                    recipientValidFuture,
                    senderValidFuture,
                    deviceValidFuture,
                    senderAccountValidFuture,
                    recipientAccountValidFuture
            ).join();

            // Check all results at once
            return recipientValidFuture.get() &&
                    senderValidFuture.get() &&
                    deviceValidFuture.get() &&
                    senderAccountValidFuture.get() &&
                    recipientAccountValidFuture.get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during parallel validation", e);
            return false;
        }
    }
}