package ee.digit25.detector.domain.person.external;

import ee.bitweb.core.retrofit.RetrofitRequestExecutor;
import ee.digit25.detector.domain.person.external.api.Person;
import ee.digit25.detector.domain.person.external.api.PersonApi;
import ee.digit25.detector.domain.person.external.api.PersonApiProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonRequester {

    private final PersonApi api;
    private final PersonApiProperties properties;

    // Cache for single person requests
    private final Map<String, CachedPerson> personCache = new ConcurrentHashMap<>();

    // Cache expiration time in milliseconds (30 seconds)
    private static final long CACHE_EXPIRATION_MS = 30_000;

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 100;

    /**
     * Get person by person code with caching and retry
     */
    public Person get(String personCode) {
        // Check cache first
        CachedPerson cachedPerson = personCache.get(personCode);
        long now = System.currentTimeMillis();

        if (cachedPerson != null && (now - cachedPerson.timestamp) < CACHE_EXPIRATION_MS) {
            log.debug("Cache hit for person {}", personCode);
            return cachedPerson.person;
        }

        log.debug("Requesting person with personCode {}", personCode);

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Person person = RetrofitRequestExecutor.executeRaw(api.get(properties.getToken(), personCode));
            // Cache the result
            personCache.put(personCode, new CachedPerson(person, now));
            return person;
        }

        // This should never be reached due to the exception in the retry loop
        throw new RuntimeException("Failed to get person after retries");
    }

    /**
     * Get multiple persons with batch processing
     */
    public List<Person> get(List<String> personCodes) {
        if (personCodes == null || personCodes.isEmpty()) {
            return new ArrayList<>();
        }

        log.debug("Requesting {} persons", personCodes.size());

        // Try to get as many persons from cache as possible
        List<Person> result = new ArrayList<>(personCodes.size());
        List<String> remaining = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String code : personCodes) {
            CachedPerson cachedPerson = personCache.get(code);
            if (cachedPerson != null && (now - cachedPerson.timestamp) < CACHE_EXPIRATION_MS) {
                result.add(cachedPerson.person);
            } else {
                remaining.add(code);
            }
        }

        // If we have all results from cache, return immediately
        if (remaining.isEmpty()) {
            log.debug("All {} persons found in cache", personCodes.size());
            return result;
        }

        // Otherwise fetch the remaining ones from API
        log.debug("Fetching {} persons from API (found {} in cache)",
                remaining.size(), personCodes.size() - remaining.size());

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            List<Person> apiResult = RetrofitRequestExecutor.executeRaw(
                    api.get(properties.getToken(), remaining));

            // Cache the results
            for (Person person : apiResult) {
                personCache.put(person.getPersonCode(), new CachedPerson(person, now));
            }

            result.addAll(apiResult);
            return result;

        }

        throw new RuntimeException("Failed to get persons after retries");
    }

    /**
     * Get paginated persons list
     */
    public List<Person> get(int pageNumber, int pageSize) {
        log.debug("Requesting persons page {} of size {}", pageNumber, pageSize);

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            List<Person> persons = RetrofitRequestExecutor.executeRaw(
                    api.get(properties.getToken(), pageNumber, pageSize));

            // Cache all persons
            long now = System.currentTimeMillis();
            for (Person person : persons) {
                personCache.put(person.getPersonCode(), new CachedPerson(person, now));
            }

            return persons;

        }

        throw new RuntimeException("Failed to get persons page after retries");
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