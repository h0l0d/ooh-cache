package ru.dkovalev.cache;

import ru.dkovalev.cache.impl.MappedFile;
import ru.dkovalev.cache.impl.Segment;
import ru.dkovalev.cache.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.abs;

public class OffHeapFixedCache<K extends Serializable, V extends Serializable>
        implements Cache<K, V> {

    public static final String FILE_NAME = "cache.bin";
    private static final int DEFAULT_SEGMENT_COUNT = 16;
    private static final int DEFAULT_SEGMENT_CAPACITY = 256;

    private final MappedFile mmap;
    private final Segment[] segments;
    private final int keySize;
    private final int valueSize;
    private final int segmentCount;
    private final int entrySize;
    private final Thread cleanupThread;
    private final CountDownLatch cleanupThreadStarted = new CountDownLatch(1);

    private enum EntryState {
        EMPTY,
        ACTIVE,
        DELETED;

        byte toByte() {
            return (byte)ordinal();
        }

        static EntryState fromByte(byte b) {
            return values()[b];
        }
    }

    private final static class Entry<K, V> {
        final EntryState state;
        final K key;
        final V value;
        final long timestampMillis;
        final long ttlMillis;

        Entry(EntryState state, K key, V value, long timestampMillis, long ttlMillis) {
            this.state = state;
            this.key = key;
            this.value = value;
            this.timestampMillis = timestampMillis;
            this.ttlMillis = ttlMillis;
        }

        static int calculateSize(int keySize, int valueSize) {
            return Byte.BYTES       // state
                    + keySize       // key
                    + valueSize     // value
                    + Long.BYTES    // timestampMillis
                    + Long.BYTES;   // ttlMillis
        }
    }

    private static final class IndexedEntry<K, V> {
        final int index;
        final Entry<K, V> entry;

        IndexedEntry(int index, Entry<K, V> entry) {
            this.index = index;
            this.entry = entry;
        }
    }

    private OffHeapFixedCache(int keySize, int valueSize, int segmentCount, int segmentCapacity) throws IOException {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.entrySize = Entry.calculateSize(keySize, valueSize);
        this.segmentCount = segmentCount;
        int segmentSize = segmentCapacity * entrySize;

        File tmp = Files.createTempFile(FILE_NAME, null).toFile();
        mmap = new MappedFile(tmp, segmentCount * segmentSize);
        segments = new Segment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(i, segmentSize, segmentCapacity);
        }

        cleanupThread = new Cleaner();
    }


    @SuppressWarnings("unused")
    public static <K extends Serializable, V extends Serializable> Cache<K, V> newInstance(int keySize, int valueSize) throws IOException {
        return newInstance(keySize, valueSize, DEFAULT_SEGMENT_COUNT, DEFAULT_SEGMENT_CAPACITY);
    }

    public static <K extends Serializable, V extends Serializable> Cache<K, V> newInstance(
        int keySize,
        int valueSize,
        int segmentCount,
        int segmentCapacity
    ) throws IOException {
        OffHeapFixedCache<K, V> cache = new OffHeapFixedCache<>(keySize, valueSize, segmentCount, segmentCapacity);
        // safe publication
        cache.cleanupThread.start();
        try {
            cache.cleanupThreadStarted.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return cache;
    }

    @Override
    public V get(K k) {
        int hashCode = hashCode(k);
        Segment segment = segmentFor(hashCode);

        try (Segment.LockHolder ignored = segment.acquireReadLock()) {
            byte[] buf = new byte[entrySize];
            for (int i = 0; i < segment.getCapacity(); i++) {
                readEntry(segment, i, buf);
                EntryState state = EntryState.fromByte(buf[0]);
                if (state == EntryState.ACTIVE) {
                    Entry<K, V> entry = deserializeEntry(buf);
                    if (entry.key.equals(k)) {
                        return entry.value;
                    }
                }
            }
        }

        return null;
    }

    @Override
    public V put(K k, V v) {
        return put(k, v, TimeUnit.MINUTES, 1);
    }

    @Override
    public V put(K k, V v, TimeUnit timeUnit, long ttl) {
        int hashCode = hashCode(k);
        Segment segment = segmentFor(hashCode);

        try (Segment.LockHolder ignored = segment.acquireWriteLock()) {
            byte[] buf = new byte[entrySize];
            IndexedEntry<K, V> prevEntry = findEntry(k, segment, buf);
            if (prevEntry != null) {
                updateEntryValue(segment, prevEntry.index, v);
                return prevEntry.entry.value;
            }
            for (int i = 0; i < segment.getCapacity(); i++) {
                readEntry(segment, i, buf);
                EntryState entryState = EntryState.fromByte(buf[0]);
                if (entryState == EntryState.DELETED || entryState == EntryState.EMPTY) {
                    writeEntry(segment, i, new Entry<>(EntryState.ACTIVE, k, v, System.currentTimeMillis(), timeUnit.toMillis(ttl)));
                    segment.incCount();
                    return null;
                }
            }
        }

        throw new IllegalStateException(String.format("Segment %s is full", segment.toString()));
    }

    @Override
    public void close() {
        try {
            cleanupThread.interrupt();
            cleanupThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        mmap.close();
    }

    private void readEntry(Segment segment, int entryIndex, byte[] buf) {
        mmap.get((int) segment.getOffset() + entryIndex * entrySize, buf);
    }

    private Entry<K, V> deserializeEntry(byte[] buf) {
        EntryState state = EntryState.fromByte(buf[0]);
        K key = SerializationUtils.deserialize(buf, 1, keySize);
        V value = SerializationUtils.deserialize(buf, 1 + keySize, valueSize);
        long timestampMillis = bytesToLong(buf, 1 + keySize + valueSize);
        long ttlNanos = bytesToLong(buf, 1 + keySize + valueSize + Long.BYTES);
        return new Entry<>(state, key, value, timestampMillis, ttlNanos);
    }

    private void writeEntry(Segment segment, int entryIndex, Entry<K, V> entry) {
        byte[] keyBuf = SerializationUtils.serialize(entry.key);
        byte[] valueBuf = SerializationUtils.serialize(entry.value);
        int entryOffset = (int)segment.getOffset() + entryIndex * entrySize;
        mmap.put(entryOffset, new byte[] {entry.state.toByte()});
        mmap.put(entryOffset + 1, keyBuf);
        mmap.put(entryOffset + 1 + keySize, valueBuf);
        mmap.put(entryOffset + 1 + keySize + valueSize, longToBytes(entry.timestampMillis));
        mmap.put(entryOffset + 1 + keySize + valueSize + Long.BYTES, longToBytes(entry.ttlMillis));
    }

    private void updateEntryState(Segment segment, int entryIndex, EntryState state) {
        int entryOffset = (int)segment.getOffset() + entryIndex * entrySize;
        mmap.put(entryOffset, new byte[] {state.toByte()});
    }

    private void updateEntryValue(Segment segment, int entryIndex, V value) {
        byte[] valueBuf = SerializationUtils.serialize(value);
        int entryOffset = (int)segment.getOffset() + entryIndex * entrySize;
        mmap.put(entryOffset + 1 + keySize, valueBuf);
    }

    private long bytesToLong(byte[] bytes, int offset) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, offset, Long.BYTES);
        buffer.flip(); //need flip
        return buffer.getLong();
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private IndexedEntry<K, V> findEntry(K k, Segment segment, byte[] buf) {
        for (int i = 0; i < segment.getCapacity(); i++) {
            readEntry(segment, i, buf);
            EntryState state = EntryState.fromByte(buf[0]);
            if (state == EntryState.ACTIVE) {
                Entry<K, V> entry = deserializeEntry(buf);
                if (entry.key.equals(k)) {
                    return new IndexedEntry<>(i, entry);
                }
            }
        }
        return null;
    }

    private static int hashCode(Object key) {
        // spreads (XORs) higher bits of hash to lower
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    private Segment segmentFor(int hashCode) {
        return segments[abs(hashCode) % segmentCount];
    }

    @Override
    public long size() {
        int count = 0;
        for (Segment segment : segments) {
            try (Segment.LockHolder ignored = segment.acquireReadLock()) {
                count += segment.getCount();
            }
        }
        return count;
    }

    private void removeExpired() {
        for (Segment segment : segments) {
            try (Segment.LockHolder ignored = segment.acquireWriteLock()) {
                byte[] buf = new byte[entrySize];
                for (int i = 0; i < segment.getCapacity(); i++) {
                    readEntry(segment, i, buf);
                    EntryState state = EntryState.fromByte(buf[0]);
                    if (state == EntryState.ACTIVE) {
                        Entry<K, V> entry = deserializeEntry(buf);
                        if (entry.timestampMillis + entry.ttlMillis <= System.currentTimeMillis()) {
                            updateEntryState(segment, i, EntryState.DELETED);
                        }
                    }
                }
            }
        }
    }

    private class Cleaner extends Thread {
        Cleaner() {
            super("OffHeapFixedCache cleaner");
        }

        @Override
        public void run() {
            cleanupThreadStarted.countDown();
            while (!isInterrupted()) {
                try {
                    removeExpired();
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
