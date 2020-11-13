package ru.dkovalev.cache.impl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final int index;
    private final long offset;
    private final int capacity;
    private int count;

    public Segment(int index, long size, int capacity) {
        this.index = index;
        this.offset = index * size;
        this.capacity = capacity;
    }

    public int getIndex() {
        return index;
    }

    public long getOffset() {
        return offset;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getCount() {
        return count;
    }

    public void incCount() {
        count++;
    }

    public static class LockHolder implements AutoCloseable {
        private final Lock lock;

        public LockHolder(Lock lock) {
            this.lock = lock;
            lock.lock();
        }

        @Override
        public void close(){
            lock.unlock();
        }
    }

    public LockHolder acquireReadLock() {
        return new LockHolder(rwLock.readLock());
    }

    public LockHolder acquireWriteLock() {
        return new LockHolder(rwLock.writeLock());
    }
}
