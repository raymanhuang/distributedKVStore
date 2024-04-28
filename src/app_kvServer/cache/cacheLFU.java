package app_kvServer.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashSet;

public class cacheLFU implements cacheInterface {
    private final int capacity;
    private final Map<String, String> keyToValueMap;
    private final Map<String, Integer> keyToFrequencyMap;
    private final Map<Integer, LinkedHashSet<String>> frequencyToKeysMap;
    private int minFrequency;

    public cacheLFU(int capacity) {
        this.capacity = capacity;
        this.keyToValueMap = new HashMap<>();
        this.keyToFrequencyMap = new HashMap<>();
        this.frequencyToKeysMap = new HashMap<>();
        this.minFrequency = 0;
    }

    @Override
    public synchronized String get(String key) {
        if (!keyToValueMap.containsKey(key)) {
            return null;
        }

        int currentFrequency = keyToFrequencyMap.get(key);
        frequencyToKeysMap.get(currentFrequency).remove(key);
        if (frequencyToKeysMap.get(currentFrequency).isEmpty()) {
            frequencyToKeysMap.remove(currentFrequency);
            if (minFrequency == currentFrequency) {
                minFrequency++;
            }
        }

        int newFrequency = currentFrequency + 1;
        frequencyToKeysMap.computeIfAbsent(newFrequency, k -> new LinkedHashSet<>()).add(key);
        keyToFrequencyMap.put(key, newFrequency);

        return keyToValueMap.get(key);
    }

    @Override
    public synchronized void put(String key, String value) {
        if (capacity == 0) {
            return;
        }

        if (keyToValueMap.containsKey(key)) {
            keyToValueMap.put(key, value);
            get(key); // update frequency using get
            return;
        }

        if (keyToValueMap.size() >= capacity) {
            String leastFrequentKey = frequencyToKeysMap.get(minFrequency).iterator().next();
            frequencyToKeysMap.get(minFrequency).remove(leastFrequentKey);
            if (frequencyToKeysMap.get(minFrequency).isEmpty()) {
                frequencyToKeysMap.remove(minFrequency);
            }
            keyToValueMap.remove(leastFrequentKey);
            keyToFrequencyMap.remove(leastFrequentKey);
        }

        keyToValueMap.put(key, value);
        keyToFrequencyMap.put(key, 1);
        frequencyToKeysMap.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(key);
        minFrequency = 1;
    }

    @Override
    public synchronized void delete(String key) {
        if (!keyToValueMap.containsKey(key)) {
            return;
        }

        int currentFrequency = keyToFrequencyMap.get(key);
        frequencyToKeysMap.get(currentFrequency).remove(key);
        if (frequencyToKeysMap.get(currentFrequency).isEmpty()) {
            frequencyToKeysMap.remove(currentFrequency);
            if (minFrequency == currentFrequency) {
                minFrequency++;
            }
        }

        keyToValueMap.remove(key);
        keyToFrequencyMap.remove(key);
    }

    @Override
    public synchronized void clear() {
        keyToValueMap.clear();
        keyToFrequencyMap.clear();
        frequencyToKeysMap.clear();
        minFrequency = 0;
    }

    @Override
    public synchronized int size() {
        return keyToValueMap.size();
    }

    @Override
    public synchronized void printCacheContents() {
        System.out.println("Cache Contents:");
        for (Map.Entry<String, String> entry : keyToValueMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Integer frequency = keyToFrequencyMap.get(key);
            System.out.println("Key: " + key + ", Value: " + value + ", Frequency: " + frequency);
        }
    }
}
