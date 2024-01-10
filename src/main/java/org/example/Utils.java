package org.example;


public class Utils {
    public static int compareKeys(byte[] key1, byte[] key2) {
        if (key1.length != key2.length)
            throw new RuntimeException("key1 and key2 are of different lengths!");
        for (int i = 0; i < key1.length; i++) {
            int cmp = Integer.compare(Byte.toUnsignedInt(key1[i]), Byte.toUnsignedInt(key2[i]));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
