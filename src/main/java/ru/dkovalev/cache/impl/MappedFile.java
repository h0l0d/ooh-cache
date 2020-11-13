package ru.dkovalev.cache.impl;

import ru.dkovalev.cache.util.UnsafeUtils;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

public class MappedFile implements AutoCloseable {

    private static final Unsafe unsafe = UnsafeUtils.getUnsafe();

    private final File file;
    private final RandomAccessFile raFile;
    private final MappedByteBuffer buffer;

    public MappedFile(File file, long size) throws IOException {
        this.file = file;
        this.raFile = new RandomAccessFile(file, "rw");
        raFile.setLength(size);

        this.buffer = raFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raFile.length());
    }

    private void clear() {
        ((sun.nio.ch.DirectBuffer) buffer).cleaner().clean();
    }

    public void get(int srcPosition, byte[] dst) {
        get(srcPosition, dst, 0, dst.length);
    }

    public void get(int srcPosition, byte[] dst, int offset, int length) {
        long srcAddress = UnsafeUtils.getByteBufferAddress(buffer) + srcPosition;
        unsafe.copyMemory(
                null, srcAddress,
                dst, unsafe.arrayBaseOffset(byte[].class),
                length);
    }

    public void put(int dstPosition, byte[] src) {
        put(dstPosition, src, 0, src.length);
    }

    public void put(int dstPosition, byte[] src, int srcOffset, int length) {
        long dstAddress = UnsafeUtils.getByteBufferAddress(buffer) + dstPosition;
        unsafe.copyMemory(
                src, unsafe.arrayBaseOffset(byte[].class) + srcOffset,
                null, dstAddress,
                length);
    }


    @Override
    public void close() {
        clear();
        try {
            raFile.close();
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
