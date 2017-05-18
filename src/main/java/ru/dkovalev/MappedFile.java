package ru.dkovalev;

import ru.dkovalev.util.UnsafeUtils;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedFile {

    private static final Unsafe unsafe = UnsafeUtils.getUnsafe();

    private MappedByteBuffer buffer;

    public MappedFile(String name, long size) throws IOException {
        RandomAccessFile f = new RandomAccessFile(name, "rw");
        f.setLength(size);

        buffer = f.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, f.length());
    }

    public void clear() {
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

}
