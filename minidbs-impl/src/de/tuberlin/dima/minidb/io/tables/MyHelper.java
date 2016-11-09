package de.tuberlin.dima.minidb.io.tables;

/**
 * Created by arbuzinside on 30.10.2015.
 */
public class MyHelper {


    /**
     * Byte array to int.
     * Byte array should be in Little Endian
     *
     * @param b the byte
     * @return the int
     */
    public static int byteArrayToInt(byte[] b) {
        return b[0] & 0xFF |
                (b[1] & 0xFF) << 8 |
                (b[2] & 0xFF) << 16 |
                (b[3] & 0xFF) << 24;
    }


    public static int byteArrayToInt(byte[] b, int offset) {

        try {

            return b[offset] & 0xFF |
                    (b[offset + 1] & 0xFF) << 8 |
                    (b[offset + 2] & 0xFF) << 16 |
                    (b[offset + 3] & 0xFF) << 24;

        } catch (NullPointerException e) {
            System.out.println(b);

        }

        return 0;
    }


    /**
     * To bytes in Little Endian format.
     *
     * @param i the i
     * @return the byte[]
     */
    public static byte[] intToBytes(int value) {
        byte[] result = new byte[4];

        result[0] = (byte) (value); /* value >> 0 */
        result[1] = (byte) (value >> 8);
        result[2] = (byte) (value >> 16);
        result[3] = (byte) (value >> 24);

        return result;
    }

    /**
     * get Int from byte array starting from offset
     * Little Endian format
     *
     * @param b
     * @param offset
     * @return the int
     */

    public static byte[] intToBytes(byte[] array, int value, int offset) {

        array[offset] = (byte) (value);
        array[offset + 1] = (byte) (value >> 8);
        array[offset + 2] = (byte) (value >> 16);
        array[offset + 3] = (byte) (value >> 24);

        return array;
    }


    public static long getLong(byte[] array, int offset) {
        return
                ((long)(array[offset+7]& 0xff)   << 56) |
                        ((long)(array[offset+6] & 0xff) << 48) |
                        ((long)(array[offset+5] & 0xff) << 40) |
                        ((long)(array[offset+4] & 0xff) << 32) |
                        ((long)(array[offset+3] & 0xff) << 24) |
                        ((long)(array[offset+2] & 0xff) << 16) |
                        ((long)(array[offset+1] & 0xff) << 8) |
                        ((long)(array[offset] & 0xff));
    }










}
