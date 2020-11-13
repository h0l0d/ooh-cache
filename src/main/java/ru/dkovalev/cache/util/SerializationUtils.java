package ru.dkovalev.cache.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class SerializationUtils {

    public static void serialize(Serializable obj, OutputStream outputStream) {
        try (ObjectOutputStream out = new ObjectOutputStream(outputStream)) {
            out.writeObject(obj);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static byte[] serialize(Serializable obj) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        serialize(obj, baos);
        return baos.toByteArray();
    }

    public static <T> T deserialize(InputStream inputStream) {
        try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;

        } catch (ClassNotFoundException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T> T deserialize(byte[] objectData) {
        return SerializationUtils.deserialize(new ByteArrayInputStream(objectData));
    }

    public static <T> T deserialize(byte[] objectData, int offset, int length) {
        return SerializationUtils.deserialize(new ByteArrayInputStream(objectData, offset, length));
    }

}
