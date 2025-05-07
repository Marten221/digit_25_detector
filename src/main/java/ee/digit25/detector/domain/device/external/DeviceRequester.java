package ee.digit25.detector.domain.device.external;

import ee.bitweb.core.retrofit.RetrofitRequestExecutor;
import ee.digit25.detector.domain.device.external.api.Device;
import ee.digit25.detector.domain.device.external.api.DeviceApi;
import ee.digit25.detector.domain.device.external.api.DeviceApiProperties;
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
public class DeviceRequester {

    private final DeviceApi api;
    private final DeviceApiProperties properties;

    // Cache for single device requests
    private final Map<String, CachedDevice> deviceCache = new ConcurrentHashMap<>();

    // Cache expiration time in milliseconds (1 minute - devices change state less frequently)
    private static final long CACHE_EXPIRATION_MS = 60_000;

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 100;

    /**
     * Get device by MAC address with caching and retry
     */
    public Device get(String mac) {
        // Check cache first
        CachedDevice cachedDevice = deviceCache.get(mac);
        long now = System.currentTimeMillis();

        if (cachedDevice != null && (now - cachedDevice.timestamp) < CACHE_EXPIRATION_MS) {
            log.debug("Cache hit for device {}", mac);
            return cachedDevice.device;
        }

        log.debug("Requesting device with mac {}", mac);

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Device device = RetrofitRequestExecutor.executeRaw(api.get(properties.getToken(), mac));
            // Cache the result
            deviceCache.put(mac, new CachedDevice(device, now));
            return device;
        }

        // This should never be reached due to the exception in the retry loop
        throw new RuntimeException("Failed to get device after retries");
    }

    /**
     * Get multiple devices with batch processing
     */
    public List<Device> get(List<String> macs) {
        if (macs == null || macs.isEmpty()) {
            return new ArrayList<>();
        }

        log.debug("Requesting {} devices", macs.size());

        // Try to get as many devices from cache as possible
        List<Device> result = new ArrayList<>(macs.size());
        List<String> remaining = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String mac : macs) {
            CachedDevice cachedDevice = deviceCache.get(mac);
            if (cachedDevice != null && (now - cachedDevice.timestamp) < CACHE_EXPIRATION_MS) {
                result.add(cachedDevice.device);
            } else {
                remaining.add(mac);
            }
        }

        // If we have all results from cache, return immediately
        if (remaining.isEmpty()) {
            log.debug("All {} devices found in cache", macs.size());
            return result;
        }

        // Otherwise fetch the remaining ones from API
        log.debug("Fetching {} devices from API (found {} in cache)",
                remaining.size(), macs.size() - remaining.size());

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            List<Device> apiResult = RetrofitRequestExecutor.executeRaw(
                    api.get(properties.getToken(), remaining));

            // Cache the results
            for (Device device : apiResult) {
                deviceCache.put(device.getMac(), new CachedDevice(device, now));
            }

            result.addAll(apiResult);
            return result;

        }

        throw new RuntimeException("Failed to get devices after retries");
    }

    /**
     * Get paginated devices list
     */
    public List<Device> get(int pageNumber, int pageSize) {
        log.debug("Requesting devices page {} of size {}", pageNumber, pageSize);

        // Implement retry logic
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            List<Device> devices = RetrofitRequestExecutor.executeRaw(
                    api.get(properties.getToken(), pageNumber, pageSize));

            // Cache all devices
            long now = System.currentTimeMillis();
            for (Device device : devices) {
                deviceCache.put(device.getMac(), new CachedDevice(device, now));
            }

            return devices;

        }

        throw new RuntimeException("Failed to get devices page after retries");
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