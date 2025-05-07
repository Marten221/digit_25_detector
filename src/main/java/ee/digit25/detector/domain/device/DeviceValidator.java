package ee.digit25.detector.domain.device;

import ee.digit25.detector.domain.device.external.DeviceRequester;
import ee.digit25.detector.domain.device.external.api.Device;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceValidator {

    private final DeviceRequester requester;

    // Simple cache to reduce redundant API calls
    // Consider using a proper caching solution like Caffeine for production
    private final Map<String, CachedDevice> deviceCache = new ConcurrentHashMap<>();

    // 1 minute cache expiration - device blacklist status less likely to change frequently
    private static final long CACHE_EXPIRATION_MS = 60000;

    public boolean isValid(String mac) {
        log.debug("Validating device {}", mac);
        return !isBlacklisted(mac);
    }

    public boolean isBlacklisted(String mac) {
        Device device = getDevice(mac);
        boolean isBlacklisted = device.getIsBlacklisted();

        if (isBlacklisted) {
            log.debug("Device {} is blacklisted", mac);
        }

        return isBlacklisted;
    }

    /**
     * Gets device info with caching to reduce API calls
     */
    private Device getDevice(String mac) {
        CachedDevice cachedDevice = deviceCache.get(mac);
        long now = System.currentTimeMillis();

        if (cachedDevice != null && (now - cachedDevice.timestamp) < CACHE_EXPIRATION_MS) {
            log.debug("Cache hit for device {}", mac);
            return cachedDevice.device;
        }

        log.debug("Cache miss for device {}, fetching from API", mac);
        Device device = requester.get(mac);
        deviceCache.put(mac, new CachedDevice(device, now));
        return device;
    }

    /**
     * Helper class for caching device data with timestamps
     */
    private static class CachedDevice {
        private final Device device;
        private final long timestamp;

        public CachedDevice(Device device, long timestamp) {
            this.device = device;
            this.timestamp = timestamp;
        }
    }
}