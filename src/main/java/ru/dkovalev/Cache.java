package ru.dkovalev;

import java.util.concurrent.TimeUnit;

public interface Cache<K, V> {

    /**
     * Puts new element, returns old value or null
     * By default 1 minute is used for TTL
     */
    V put(K k, V v);

    V put(K k, V v, TimeUnit timeUnit, long ttl);

    /**
     * Returns value by key, or null if no value found
     */
    V get(K k);


    /**
     * Cleans whole cache
     */
    void clear();

    long size();

}