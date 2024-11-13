package redisRDB;

import redisProtocol.DataMaps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ReadRDBFile {
    private DataMaps dataMaps;

    public ReadRDBFile(final DataMaps dataMaps) {
        this.dataMaps = dataMaps;
    }

    public void readFromFile() throws FileNotFoundException {
        try {
            final String filePath = dataMaps.getConfigMap().get("dir") + "/" + dataMaps.getConfigMap().get("dbfilename");
            System.out.println("filePath is " + filePath);
            final File dbfile = new File(filePath);
            final InputStream inputStream = new FileInputStream(dbfile);
            reader(inputStream);
        } catch (final Exception e) {

        }
    }


    public void reader(final InputStream inputStream) {
        try {
            System.out.println("reading rdb " + System.currentTimeMillis());
            Boolean startReading = Boolean.FALSE;
            int read;
            while ((read = inputStream.read()) != -1) {

                if (read == 0xFB) {
                    System.out.println("starting reading data from rdb");
                    getLen(inputStream);
                    getLen(inputStream);
                    startReading = Boolean.TRUE;
                } else if (startReading && read == 0xFC) {
                    final Long timeStamp = getTimestamp(inputStream, 8);
                    inputStream.read();
                    setKeyValuePair(inputStream, dataMaps, timeStamp);
                } else if (startReading && read == 0xFD) {
                    final Long timeStamp = getTimestamp(inputStream, 4) * 100;
                    inputStream.read();
                    setKeyValuePair(inputStream, dataMaps, timeStamp);
                } else if (startReading && read == 0) {
                    setKeyValuePair(inputStream, dataMaps, -1L);
                } else if (read == 0xFF) {
                   inputStream.read(new byte[8]);
                   break;
                }
            }

        } catch (final Exception e) {
            System.out.println("error is :: " + e.getMessage());
        }
        System.out.println("bye bye " + System.currentTimeMillis());
    }

    private void setKeyValuePair(final InputStream inputStream, final DataMaps dataMaps, final Long ttl) throws IOException {
        final String key = getStringData(inputStream);
        System.out.println("key from rdb is " + key);
        final String value = getStringData(inputStream);
        System.out.println("value from rdb is " + value);
        synchronized (key) {
            dataMaps.getStringMap().put(key, value);
            if (ttl > 0) dataMaps.getKeyTtl().put(key, ttl);
        }
    }


    private String getStringData(final InputStream inputStream) throws IOException {
        final int len = getLen(inputStream);
        final byte[] key_bytes = new byte[len];
        inputStream.read(key_bytes);
        return new String(key_bytes);
    }

    private Long getTimestamp(final InputStream inputStream, final int bytesToRead) throws IOException {
        final byte[] timeStampBytes = new byte[bytesToRead];
        inputStream.read(timeStampBytes);
        return ByteBuffer.wrap(timeStampBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static int getLen(final InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        final int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
        if (len_encoding_bit == 0) {
            len = read & 0b00111111;
        } else if (len_encoding_bit == 1) {
            final int extra_len = inputStream.read();
            len = ((read & 0b00111111) << 8) + extra_len;
        } else if (len_encoding_bit == 2) {
            final byte[] extra_len = new byte[4];
            inputStream.read(extra_len);
            len = ByteBuffer.wrap(extra_len).getInt();
        }
        return len;
    }
}