package ru.dkovalev.cache

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class OffHeapFixedCacheTest {

    lateinit var cache: Cache<String, String>

    @AfterEach
    internal fun tearDown() {
        cache.close()
    }

    @Test
    @Throws(Exception::class)
    fun `should overwrite value on put and return previous value`() {
        cache = OffHeapFixedCache.newInstance<String, String>(10, 100, 2, 2)

        var prevValue = cache.put("key", "value")
        assertThat(prevValue, nullValue())


        var value = cache["key"]
        assertThat(value, equalTo("value"))

        prevValue = cache.put("key", "value1")
        assertThat(prevValue, equalTo("value"))

        value = cache["key"]
        assertThat(value, equalTo("value1"))
    }

    @Test
    @Throws(Exception::class)
    @Timeout(3)
    fun `concurrent put`() {
        val segmentCapacity = 5
        cache = OffHeapFixedCache.newInstance<String, String>(20, 100, segmentCapacity, 2)

        val startedSignal = CountDownLatch(segmentCapacity)
        val readySignal = CountDownLatch(1)
        val finishedSignal = CountDownLatch(segmentCapacity)

        for (i in 1..segmentCapacity) {
            Thread(Runnable {
                startedSignal.countDown()
                readySignal.await()
                cache.put("key$i", "value$i")
                finishedSignal.countDown()
            }).start()
        }
        startedSignal.await()
        readySignal.countDown()
        finishedSignal.await()

        assertThat(cache.size(), equalTo(segmentCapacity.toLong()))
        for (i in 1..segmentCapacity) {
            assertThat(cache["key$i"], equalTo("value$i"))
        }
    }

    @Test
    @Throws(Exception::class)
    fun `should removed expired entry`() {
        cache = OffHeapFixedCache.newInstance<String, String>(10, 100, 2, 2)

        cache.put("key", "value", TimeUnit.SECONDS, 1)
        var value = cache["key"]
        assertThat(value, equalTo("value"))

        // todo: add remove listeners to cache and await expired entry removal instead of unstable sleep
        TimeUnit.SECONDS.sleep(3)
        value = cache["key"]
        assertThat(value, nullValue())
    }
}