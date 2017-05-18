package ru.dkovalev;

import ru.dkovalev.util.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OffHeapFixedCache<K extends Serializable, V extends Serializable> implements Cache<K, V> {

    public static final String FILE_NAME = "cache.bin";
    private static final int DEFAULT_SEGMENT_COUNT = 16;
    private static final int DEFAULT_SEGMENT_CAPACITY = 256;

    private MappedFile mmap;
    private Segment[] segments;
    private int keySize;
    private int valueSize;
    private int segmentCount;
    private int entrySize;
    private Thread cleanupThread;

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

    private static final class Entry<K, V> {
        EntryState state;
        K key;
        V value;
        long timestampMillis;
        long ttlMillis;

        Entry(EntryState state, K key, V value, long timestampMillis, long ttlMillis) {
            this.state = state;
            this.key = key;
            this.value = value;
            this.timestampMillis = timestampMillis;
            this.ttlMillis = ttlMillis;
        }

        static int calculateSize(int keySize, int valueSize) {
            return Byte.BYTES + keySize + valueSize + Long.BYTES + Long.BYTES;
        }
    }

    private static final class IndexableEntry<K, V> {
        final int index;
        final Entry<K, V> entry;

        IndexableEntry(int index, Entry<K, V> entry) {
            this.index = index;
            this.entry = entry;
        }
    }

    private static final class Segment {
        private ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private int index;
        private long offset;
        private int capacity;
        private int count;

        Segment(int index, long size, int capacity) {
            this.index = index;
            this.offset = index * size;
            this.capacity = capacity;
        }

        Lock readLock() {
            return rwLock.readLock();
        }

        Lock writeLock() {
            return rwLock.writeLock();
        }

        @Override
        public String toString() {
            return "Segment{" +
                    "index=" + index +
                    ", offset=" + offset +
                    ", capacity=" + capacity +
                    ", count=" + count +
                    '}';
        }
    }

    public OffHeapFixedCache(int keySize, int valueSize) throws IOException {
        this(keySize, valueSize, DEFAULT_SEGMENT_COUNT, DEFAULT_SEGMENT_CAPACITY);
    }

    public OffHeapFixedCache(int keySize, int valueSize, int segmentCount, int segmentCapacity) throws IOException {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.entrySize = Entry.calculateSize(keySize, valueSize);
        this.segmentCount = segmentCount;
        int segmentSize = segmentCapacity * entrySize;

        mmap = new MappedFile(FILE_NAME, segmentCount * segmentSize);
        segments = new Segment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(i, segmentSize, segmentCapacity);
        }

        cleanupThread = new Cleaner();
        cleanupThread.start();
    }

    @Override
    public V get(K k) {
        int hashCode = hashCode(k);
        Segment segment = segmentFor(hashCode);

        Lock lock = segment.readLock();
        lock.lock();
        try {
            byte[] buf = new byte[entrySize];
            for (int i = 0; i < segment.capacity; i++) {
                readEntry(segment, i, buf);
                EntryState state = EntryState.fromByte(buf[0]);
                if (state == EntryState.ACTIVE) {
                    Entry<K, V> entry = deserializeEntry(buf);
                    if (entry.key.equals(k)) {
                        return entry.value;
                    }
                }
            }
        } finally {
            lock.unlock();
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

        Lock lock = segment.writeLock();
        lock.lock();
        try {
            byte[] buf = new byte[entrySize];
            IndexableEntry<K, V> prevEntry = findEntry(k, segment, buf);
            if (prevEntry != null) {
                updateEntryValue(segment, prevEntry.index, v);
                return prevEntry.entry.value;
            }
            for (int i = 0; i < segment.capacity; i++) {
                readEntry(segment, i, buf);
                EntryState entryState = EntryState.fromByte(buf[0]);
                if (entryState == EntryState.DELETED || entryState == EntryState.EMPTY) {
                    writeEntry(segment, i, new Entry<>(EntryState.ACTIVE, k, v, System.currentTimeMillis(), timeUnit.toMillis(ttl)));
                    segment.count++;
                    return null;
                }
            }

        } finally {
            lock.unlock();
        }

        throw new IllegalStateException(String.format("Segment %s is full", segment.toString()));
    }

    private void readEntry(Segment segment, int entryIndex, byte[] buf) {
        mmap.get((int) segment.offset + entryIndex * entrySize, buf);
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
        int entryOffset = (int)segment.offset + entryIndex * entrySize;
        mmap.put(entryOffset, new byte[] {entry.state.toByte()});
        mmap.put(entryOffset + 1, keyBuf);
        mmap.put(entryOffset + 1 + keySize, valueBuf);
        mmap.put(entryOffset + 1 + keySize + valueSize, longToBytes(entry.timestampMillis));
        mmap.put(entryOffset + 1 + keySize + valueSize + Long.BYTES, longToBytes(entry.ttlMillis));
    }

    private void updateEntryState(Segment segment, int entryIndex, EntryState state) {
        int entryOffset = (int)segment.offset + entryIndex * entrySize;
        mmap.put(entryOffset, new byte[] {state.toByte()});
    }

    private void updateEntryValue(Segment segment, int entryIndex, V value) {
        byte[] valueBuf = SerializationUtils.serialize(value);
        int entryOffset = (int)segment.offset + entryIndex * entrySize;
        mmap.put(entryOffset + 1 + keySize, valueBuf);
    }

    private long bytesToLong(byte[] bytes, int offset) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, offset, Long.BYTES);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private IndexableEntry<K, V> findEntry(K k, Segment segment, byte[] buf) {
        for (int i = 0; i < segment.capacity; i++) {
            readEntry(segment, i, buf);
            EntryState state = EntryState.fromByte(buf[0]);
            if (state == EntryState.ACTIVE) {
                Entry<K, V> entry = deserializeEntry(buf);
                if (entry.key.equals(k)) {
                    return new IndexableEntry<>(i, entry);
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
        return segments[hashCode % segmentCount];
    }

    @Override
    public void clear() {
        mmap.clear();
        mmap = null;
        segments = null;

        if (cleanupThread != null) {
            try {
                cleanupThread.interrupt();
                cleanupThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public long size() {
        int count = 0;
        for (Segment segment : segments) {
            count += segment.count;
        }
        return count;
    }

    private void removeExpired() {
        for (Segment segment : segments) {
            Lock lock = segment.writeLock();
            lock.lock();
            try {
                byte[] buf = new byte[entrySize];
                for (int i = 0; i < segment.capacity; i++) {
                    readEntry(segment, i, buf);
                    EntryState state = EntryState.fromByte(buf[0]);
                    if (state == EntryState.ACTIVE) {
                        Entry<K, V> entry = deserializeEntry(buf);
                        if (entry.timestampMillis + entry.ttlMillis <= System.currentTimeMillis()) {
                            updateEntryState(segment, i, EntryState.DELETED);
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public class Cleaner extends Thread {
        Cleaner() {
            super("OffHeapFixedCache cleaner");
        }

        @Override
        public void run() {
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
