package app_kvServer.cache;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;

public class cacheLRU implements cacheInterface {
    private final int capacity;
    private final Map<String, String> cacheMap;

    public cacheLRU(int capacity) {
        this.capacity = capacity;
        this.cacheMap = Collections.synchronizedMap(new LinkedHashMap<String, String>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > cacheLRU.this.capacity;
            }
        });
    }

    @Override
    public String get(String key) {
        return cacheMap.get(key); // Access order is updated on each get call
    }

    @Override
    public void put(String key, String value) {
        cacheMap.put(key, value); // Access order is updated on each put call
    }

    @Override
    public void delete(String key) {
        cacheMap.remove(key);
    }

    @Override
    public void clear() {
        cacheMap.clear();
    }

    @Override
    public int size() {
        return cacheMap.size();
    }

    @Override
    public synchronized void printCacheContents() {
        System.out.println("Cache Contents:");
        for (Map.Entry<String, String> entry : cacheMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println("Key: " + key + ", Value: " + value);
        }
    }
    
}
