package app_kvServer.cache;

public interface cacheInterface{
    // Retrieves an entry from the cache.
    // Returns null if the key is not present.
    String get(String key);

    // Deletes an entry from the cache.
    void delete(String key);

    // Clears all the entries from the cache.
    void clear();

    // Puts an entry into the cache.
    // If the cache is full , it should evict an entry according to the specific cache strategy.
    void put(String key, String value);
    
    // Returns the number of key-value pairs currently in the cache.
    int size();

    void printCacheContents();

}