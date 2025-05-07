package ee.digit25.detector.domain.person;

import ee.digit25.detector.domain.person.external.PersonRequester;
import ee.digit25.detector.domain.person.external.api.Person;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonValidator {

    private final PersonRequester requester;

    // Simple cache to reduce redundant API calls
    // Consider using a proper caching solution like Caffeine for production
    private final Map<String, CachedPerson> personCache = new ConcurrentHashMap<>();

    // 30 second cache expiration
    private static final long CACHE_EXPIRATION_MS = 30000;

    public boolean isValid(String personCode) {
        Person person = getPerson(personCode);

        boolean isValid = !person.getBlacklisted() &&
                !person.getWarrantIssued() &&
                person.getHasContract();

        if (!isValid) {
            if (person.getBlacklisted()) {
                log.debug("Person {} is blacklisted", personCode);
            }
            if (person.getWarrantIssued()) {
                log.debug("Person {} has warrant issued", personCode);
            }
            if (!person.getHasContract()) {
                log.debug("Person {} has no contract", personCode);
            }
        }

        return isValid;
    }

    /**
     * Gets person info with caching to reduce API calls
     */
    private Person getPerson(String personCode) {
        CachedPerson cachedPerson = personCache.get(personCode);
        long now = System.currentTimeMillis();

        if (cachedPerson != null && (now - cachedPerson.timestamp) < CACHE_EXPIRATION_MS) {
            log.debug("Cache hit for person {}", personCode);
            return cachedPerson.person;
        }

        log.debug("Cache miss for person {}, fetching from API", personCode);
        Person person = requester.get(personCode);
        personCache.put(personCode, new CachedPerson(person, now));
        return person;
    }

    /**
     * Helper class for caching person data with timestamps
     */
    private static class CachedPerson {
        private final Person person;
        private final long timestamp;

        public CachedPerson(Person person, long timestamp) {
            this.person = person;
            this.timestamp = timestamp;
        }
    }
}