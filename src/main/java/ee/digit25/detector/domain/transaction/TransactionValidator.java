package ee.digit25.detector.domain.transaction;

import ee.digit25.detector.domain.account.AccountValidator;
import ee.digit25.detector.domain.device.DeviceValidator;
import ee.digit25.detector.domain.person.PersonValidator;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionValidator {

    private final PersonValidator personValidator;
    private final DeviceValidator deviceValidator;
    private final AccountValidator accountValidator;

    public boolean isLegitimate(Transaction transaction) {
        String recipient = transaction.getRecipient();
        String sender = transaction.getSender();

        if (!personValidator.isValid(recipient)) return false;
        if (!personValidator.isValid(sender)) return false;
        if (!deviceValidator.isValid(transaction.getDeviceMac())) return false;
        if (!accountValidator.isValidSenderAccount(transaction.getSenderAccount(), transaction.getAmount(), sender)) return false;
        return accountValidator.isValidRecipientAccount(transaction.getRecipientAccount(), recipient);
    }
}
