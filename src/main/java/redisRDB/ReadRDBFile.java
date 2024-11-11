package redisRDB;

import redisProtocol.DataMaps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ReadRDBFile {
    private DataMaps dataMaps;

    public ReadRDBFile(final DataMaps dataMaps) {
        this.dataMaps = dataMaps;
    }


    public void read() throws IOException {
        try {
            //final String filePath = dataMaps.getConfigMap().get("dir") + "/" + dataMaps.getConfigMap().get("dbfilename");
            //System.out.println("filePath is " + filePath);
            File dbfile = new File("/Users/hariom.sharma/pp/codecrafters-redis-java/sample.rdb");
            Boolean startReading = Boolean.FALSE;
            int read;
            InputStream inputStream = new FileInputStream(dbfile);
            while ((read = inputStream.read()) != -1) {
                if (read == 0xFB) {
                    getLen(inputStream);
                    getLen(inputStream);
                    startReading = Boolean.TRUE;
                } else if (startReading && read == 0xFC) {
                    Long timeStamp = getTimestamp(inputStream, 8);
                    setKeyValuePair(inputStream, dataMaps, timeStamp);
                } else if (startReading && read == 0xFD) {
                    Long timeStamp = getTimestamp(inputStream, 4) * 100;
                    setKeyValuePair(inputStream, dataMaps, timeStamp);
                } else if (startReading) {
                    setKeyValuePair(inputStream, dataMaps, -1L);
                }
            }

        } catch (final Exception e) {
            System.out.println("error is :: " + e.getMessage());
        }

//        Path file = Paths.get(filePath);
//        String line = "";
//        try (BufferedReader reader = Files.newBufferedReader(file)) {
//            String[] hexValues;
//            while ((line = reader.readLine()) != null) {
//                System.out.println("reading line " + line);
//                hexValues = line.trim().split("\\s+");
//                for (String hex : hexValues) {
//                    if (!hex.isEmpty()) {
//                        if ("FB".equalsIgnoreCase(hex)) {
//                            reader.readLine();
//                            reader.readLine();
//                            line = reader.readLine().trim();
//                            startReading = Boolean.TRUE;
//                        }
//                        if (startReading) {
//                            if ("FD".equalsIgnoreCase(line)) {
//                                Long ttl = Long.parseLong(readTimeStamp(reader), 16) * 100;
//                                reader.readLine();
//                                setKeyValue(ttl, reader);
//
//                            } else if ("FC".equalsIgnoreCase(line)) {
//                                Long ttl = Long.parseLong(readTimeStamp(reader), 16);
//                                reader.readLine();
//                                setKeyValue(ttl, reader);
//                            } else {
//                                setKeyValue(-1L, reader);
//
//                            }
//                        }
//
//
//                    }
//                }
//            }
//        } catch (final Exception e) {
//            e.printStackTrace();
//            System.out.println("last line was ," + line);
//            System.out.println(e.getMessage());
//        }
    }

    private void setKeyValuePair(final InputStream inputStream, final DataMaps dataMaps, final Long ttl) throws IOException {
        inputStream.read();
        final String key = getStringData(inputStream);
        final String value = getStringData(inputStream);
        synchronized (key) {
            dataMaps.getStringMap().put(key, value);
            if (ttl > 0) dataMaps.getKeyTtl().put(key, ttl);
        }
    }


    private String getStringData(final InputStream inputStream) throws IOException {
        int len = getLen(inputStream);
        byte[] key_bytes = new byte[len];
        inputStream.read(key_bytes);
        return new String(key_bytes);
    }

    private Long getTimestamp(InputStream inputStream, int bytesToRead) throws IOException {
        byte[] timeStampBytes = new byte[bytesToRead];
        inputStream.read(new byte[bytesToRead]);
        Long timeStamp = 0L;
        for (int i = bytesToRead - 1; i >= 0; i--)
            timeStamp = timeStamp + ((long) timeStampBytes[i] << (bytesToRead - i));
        return timeStamp;
    }


    private static int getLen(InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
        if (len_encoding_bit == 0) {
            len = read & 0b00111111;
        } else if (len_encoding_bit == 1) {
            int extra_len = inputStream.read();
            len = ((read & 0b00111111) << 8) + extra_len;
        } else if (len_encoding_bit == 2) {
            byte[] extra_len = new byte[4];
            inputStream.read(extra_len);
            len = ByteBuffer.wrap(extra_len).getInt();
        }
        return len;
    }

//    private void setKeyValue(final Long ttl, final BufferedReader reader) throws IOException {
//
//        final String key = readStringData(reader);
//        final String data = readStringData(reader);
//        System.out.println("key is " + key);
//        System.out.println("data is " + data);
//        synchronized (key) {
//            dataMaps.getStringMap().put(key, data);
//            if (ttl > 0) {
//                dataMaps.getKeyTtl().put(key, ttl);
//            }
//        }
//    }
//
//
//    private String readTimeStamp(final BufferedReader reader) throws IOException {
//        final String line = reader.readLine().trim();
//        final String[] hexValues = line.trim().split("\\s+");
//        String timeStamp = "";
//        for (int i = hexValues.length - 1; i >= 0; i--) timeStamp += hexValues[i];
//        return timeStamp;
//    }
//
//    private String readStringData(final BufferedReader reader) throws IOException {
//        final String line = reader.readLine().trim();
//        Long length = 0L;
//        String size = "";
//        String data = "";
//        final String[] hexValues = line.trim().split("\\s+");
//        final String firstByte = hexToBinary(hexValues[0]);
//        if (firstByte.charAt(0) == '0' && firstByte.charAt(1) == '0') {
//            for (int i = 2; i < 8; i++) size += firstByte.charAt(i);
//            length = Long.parseLong(size, 2);
//            for (int i = 1; i < hexValues.length; i++) {
//                data += (char) Integer.parseInt(hexValues[i], 16);
//            }
//
//        } else if (firstByte.charAt(0) == '0' && firstByte.charAt(1) == '1') {
//            for (int i = 2; i < 8; i++) size += firstByte.charAt(i);
//            size += hexToBinary(hexValues[1]);
//            length = Long.parseLong(size, 2);
//            for (int i = 2; i < hexValues.length; i++) {
//                data += (char) Integer.parseInt(hexValues[i], 16);
//            }
//        } else if (firstByte.charAt(0) == '1' && firstByte.charAt(1) == '0') {
//            for (int i = 1; i < 5; i++) size += hexToBinary(hexValues[i]);
//            length = Long.parseLong(size, 2);
//            for (int i = 5; i < hexValues.length; i++) {
//                data += (char) Integer.parseInt(hexValues[i], 16);
//            }
//        }
//        if (data.length() != length) throw new RuntimeException();
//        return data;
//    }
//
//
//    private String hexToBinary(final String hex) {
//        final int decimal = Integer.parseInt(hex, 16);
//        String binary = Integer.toBinaryString(decimal);
//        while (binary.length() < 8) {
//            binary = "0" + binary;
//        }
//        return binary;
//    }

}
