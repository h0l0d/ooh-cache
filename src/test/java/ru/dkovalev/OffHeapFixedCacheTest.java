package ru.dkovalev;

import org.junit.*;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class OffHeapFixedCacheTest {


    @Before
    public void setUp() throws Exception {
        new File(OffHeapFixedCache.FILE_NAME).delete();
    }

    @Test
    public void testPutAndGet() throws Exception {

        OffHeapFixedCache<String, String> cache = new OffHeapFixedCache<>(10, 100, 2, 2);

        String prevValue = cache.put("key", "value");
        assertThat(prevValue, nullValue());

        String value = cache.get("key");
        System.out.println(value);
        assertThat(value, is("value"));

        prevValue = cache.put("key", "value1");
        assertThat(prevValue, is("value"));

        value = cache.get("key");
        System.out.println(value);
        assertThat(value, is("value1"));
    }

    @Test
    public void testTtl() throws Exception {

        OffHeapFixedCache<String, String> cache = new OffHeapFixedCache<>(10, 100, 2, 2);

        cache.put("key", "value", TimeUnit.SECONDS, 2);
        String value = cache.get("key");
        System.out.println(value);
        assertThat(value, is("value"));

        TimeUnit.SECONDS.sleep(3);

        value = cache.get("key");

        assertThat(value, nullValue());
    }
}