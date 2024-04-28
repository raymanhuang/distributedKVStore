package app_kvServer.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Queue;

public class cacheFIFO implements cacheInterface {
    private final int capacity;
    private final Map<String, String> keyToValueMap;
    private final Queue<String> keyQueue;

    public cacheFIFO(int capacity) {
        this.capacity = capacity;
        this.keyToValueMap = new HashMap<>();
        this.keyQueue = new LinkedList<>();
    }

    @Override
    public synchronized String get(String key) {
        return keyToValueMap.get(key);
    }

    @Override
    public synchronized void put(String key, String value) {
        if (keyToValueMap.containsKey(key)) {
            keyToValueMap.put(key, value);
            return;
        }

        if (keyToValueMap.size() >= capacity) {
            String oldestKey = keyQueue.poll();
            if (oldestKey != null) {
                keyToValueMap.remove(oldestKey);
            }
        }

        keyToValueMap.put(key, value);
        keyQueue.offer(key);
    }

    @Override
    public synchronized void delete(String key) {
        if (keyToValueMap.containsKey(key)) {
            keyToValueMap.remove(key);
            keyQueue.remove(key);
        }
    }

    @Override
    public synchronized void clear() {
        keyToValueMap.clear();
        keyQueue.clear();
    }

    @Override
    public synchronized int size() {
        return keyToValueMap.size();
    }

    @Override
    public synchronized void printCacheContents() {
        System.out.println("Cache Contents:");
        for (Map.Entry<String, String> entry : keyToValueMap.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
    }
}