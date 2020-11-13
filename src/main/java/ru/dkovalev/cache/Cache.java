package ru.dkovalev.cache;

import java.util.concurrent.TimeUnit;

public interface Cache<K, V> extends AutoCloseable {

    /**
     * Puts new element, returns old value or null
     * By default 1 minute is used for TTL
     */
    V put(K k, V v);

    /**
     * Puts new element with given TTL, returns old value or null
     */
    V put(K k, V v, TimeUnit timeUnit, long ttl);

    /**
     * Returns value by key, or null if no value found
     */
    V get(K k);

    /**
     * Number of elements in the cache.
     */
    long size();

}