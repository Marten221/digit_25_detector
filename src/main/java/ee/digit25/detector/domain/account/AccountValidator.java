package ee.digit25.detector.domain.account;

import ee.digit25.detector.domain.account.external.AccountRequester;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountValidator {

    private final AccountRequester requester;

    public boolean isValidSenderAccount(String accountNumber, BigDecimal amount, String senderPersonCode) {
        log.info("Checking if account {} is valid sender account", accountNumber);

        if (isClosed(accountNumber)) {
            return false;
        } else if (!isOwner(accountNumber, senderPersonCode)) {
            return false;
        } else return hasBalance(accountNumber, amount);
    }

    public boolean isValidRecipientAccount(String accountNumber, String recipientPersonCode) {
        log.info("Checking if account {} is valid recipient account", accountNumber);
        if (isClosed(accountNumber)) {
            return false;
        } else return isOwner(accountNumber, recipientPersonCode);
    }

    private boolean isOwner(String accountNumber, String senderPersonCode) {
        log.info("Checking if {} is owner of account {}", senderPersonCode, accountNumber);

        return senderPersonCode.equals(requester.get(accountNumber).getOwner());
    }

    private boolean hasBalance(String accountNumber, BigDecimal amount) {
        log.info("Checking if account {} has balance for amount {}", accountNumber, amount);

        return requester.get(accountNumber).getBalance().compareTo(amount) >= 0;
    }

    private boolean isClosed(String accountNumber) {
        log.info("Checking if account {} is closed", accountNumber);

        return requester.get(accountNumber).getClosed();
    }
}
