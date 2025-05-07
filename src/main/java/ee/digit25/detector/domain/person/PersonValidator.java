package ee.digit25.detector.domain.person;

import ee.digit25.detector.domain.person.external.PersonRequester;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonValidator {

    private final PersonRequester requester;

    private boolean hasWarrantIssued(String personCode) {
        log.info("Cheking if person ({}) has a warrant issued", personCode);

        return requester.get(personCode).getWarrantIssued();
    }

    private boolean hasContract(String personCode) {
        log.info("Cheking if person ({}) has a contract", personCode);

        return requester.get(personCode).getHasContract();
    }

    private boolean isBlacklisted(String personCode) {
        log.info("Cheking if person ({}) is blacklisted", personCode);

        return requester.get(personCode).getBlacklisted();
    }

    public boolean isValid(String personCode) {
        return !isBlacklisted(personCode) && !hasWarrantIssued(personCode) && hasContract(personCode);
    }
}
