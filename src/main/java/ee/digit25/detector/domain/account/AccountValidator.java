package ee.digit25.detector.domain.account;

import ee.digit25.detector.domain.account.external.AccountRequester;
import ee.digit25.detector.domain.account.external.api.Account;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountValidator {

    private final AccountRequester requester;

    // Simple cache to reduce redundant API calls
    // Consider using a proper caching solution like Caffeine for production
    private final Map<String, CachedAccount> accountCache = new ConcurrentHashMap<>();

    // 5 second cache expiration
    private static final long CACHE_EXPIRATION_MS = 5000;

    public boolean isValidSenderAccount(String accountNumber, BigDecimal amount, String senderPersonCode) {
        log.debug("Checking if account {} is valid sender account", accountNumber);

        // Get account details, possibly from cache
        Account account = getAccount(accountNumber);

        if (account.getClosed()) {
            return false;
        } else if (!senderPersonCode.equals(account.getOwner())) {
            return false;
        } else {
            return account.getBalance().compareTo(amount) >= 0;
        }
    }

    public boolean isValidRecipientAccount(String accountNumber, String recipientPersonCode) {
        log.debug("Checking if account {} is valid recipient account", accountNumber);

        // Get account details, possibly from cache
        Account account = getAccount(accountNumber);

        if (account.getClosed()) {
            return false;
        } else {
            return recipientPersonCode.equals(account.getOwner());
        }
    }

    /**
     * Gets account info with caching to reduce API calls
     */
    private Account getAccount(String accountNumber) {
        CachedAccount cachedAccount = accountCache.get(accountNumber);
        long now = System.currentTimeMillis();

        if (cachedAccount != null && (now - cachedAccount.timestamp) < CACHE_EXPIRATION_MS) {
            log.debug("Cache hit for account {}", accountNumber);
            return cachedAccount.account;
        }

        log.debug("Cache miss for account {}, fetching from API", accountNumber);
        Account account = requester.get(accountNumber);
        accountCache.put(accountNumber, new CachedAccount(account, now));
        return account;
    }

    /**
     * Helper class for caching account data with timestamps
     */
    private static class CachedAccount {
        private final Account account;
        private final long timestamp;

        public CachedAccount(Account account, long timestamp) {
            this.account = account;
            this.timestamp = timestamp;
        }
    }
}