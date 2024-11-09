package redisRDB;

import redisProtocol.DataMaps;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ReadRDBFile {
    private DataMaps dataMaps;

    public ReadRDBFile(final DataMaps dataMaps) {
        this.dataMaps = dataMaps;
    }


    public void read() throws IOException {
        final String filePath = dataMaps.getConfigMap().get("dir") + "/" + dataMaps.getConfigMap().get("dbfilename");
        Boolean startReading = Boolean.FALSE;
        Path file = Paths.get(filePath);
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            String[] hexValues;
            while ((line = reader.readLine()) != null) {
                hexValues = line.trim().split("\\s+");
                for (String hex : hexValues) {
                    if (!hex.isEmpty()) {
                        if ("FB".equalsIgnoreCase(hex)) {
                            reader.readLine();
                            reader.readLine();
                            startReading = Boolean.TRUE;
                        }
                        if (startReading) {
                            line = reader.readLine();
                            if ("FD".equalsIgnoreCase(line)) {
                                Long ttl = Long.parseLong(readTimeStamp(reader), 16) * 100;
                                setKeyValue(ttl, reader);

                            } else if ("FC".equalsIgnoreCase(line)) {
                                Long ttl = Long.parseLong(readTimeStamp(reader), 16);
                                setKeyValue(ttl, reader);
                            } else {
                                setKeyValue(-1L, reader);

                            }
                        }


                    }
                }
            }
        }
    }


    private void setKeyValue(final Long ttl, final BufferedReader reader) throws IOException {
        reader.readLine();
        final String key = readStringData(reader);
        final String data = readStringData(reader);
        synchronized (key) {
            dataMaps.getStringMap().put(key, data);
            if (ttl > 0) {
                dataMaps.getKeyTtl().put(key, ttl);
            }
        }
    }


    private String readTimeStamp(final BufferedReader reader) throws IOException {
        final String line = reader.readLine();
        final String[] hexValues = line.trim().split("\\s+");
        String timeStamp = "";
        for (int i = hexValues.length - 1; i >= 0; i--) timeStamp += hexValues[i];
        return timeStamp;
    }

    private String readStringData(final BufferedReader reader) throws IOException {
        final String line = reader.readLine().trim().replaceAll("\\s+", "");
        Long length = 0L;
        String size = "";
        String data = "";
        final String[] hexValues = line.trim().split("\\s+");
        final String firstByte = hexToBinary(hexValues[0]);
        if (firstByte.charAt(0) == '0' && firstByte.charAt(1) == '0') {
            for (int i = 2; i < 8; i++) size += firstByte.charAt(i);
            length = Long.parseLong(size, 2);
            for (int i = 1; i < hexValues.length; i++) {
                data += (char) Integer.parseInt(hexValues[i], 16);
            }

        } else if (firstByte.charAt(0) == '0' && firstByte.charAt(1) == '1') {
            for (int i = 2; i < 8; i++) size += firstByte.charAt(i);
            size += hexToBinary(hexValues[1]);
            length = Long.parseLong(size, 2);
            for (int i = 2; i < hexValues.length; i++) {
                data += (char) Integer.parseInt(hexValues[i], 16);
            }
        } else if (firstByte.charAt(0) == '1' && firstByte.charAt(1) == '0') {
            for (int i = 1; i < 5; i++) size += hexToBinary(hexValues[i]);
            length = Long.parseLong(size, 2);
            for (int i = 5; i < hexValues.length; i++) {
                data += (char) Integer.parseInt(hexValues[i], 16);
            }
        }
        if (data.length() != length) throw new RuntimeException();
        return data;
    }


    private String hexToBinary(final String hex) {
        int decimal = Integer.parseInt(hex, 16);
        return Integer.toBinaryString(decimal);
    }

}
