package example.benchmarks;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GenerateNYCFieldUniqueValues {

    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        Map<Object, Object> uniqueValues = new ConcurrentHashMap<>(1_000_000);
        AtomicLong count = new AtomicLong(0);

        NYCUniqueFieldValueConfig uniqueFieldValueConfig = new NYCUniqueFieldValueConfig(args);
        long bytesPerThread = (new File(uniqueFieldValueConfig.getInputFileName()).length()) / uniqueFieldValueConfig.getNumWriterThreads();
        long currentByteStart = 0;
        try (ExecutorService threadPool = Executors.newFixedThreadPool(uniqueFieldValueConfig.getNumWriterThreads())) {
            for (int i = 1; i <= uniqueFieldValueConfig.getNumWriterThreads(); i++) {
                threadPool.submit(new WriterThread(currentByteStart, bytesPerThread,
                        uniqueFieldValueConfig,
                        uniqueValues,
                        count
                ));
                currentByteStart += bytesPerThread;
            }
        }
        System.out.printf("\nWriting %s Unique Values to file", uniqueValues.size());

        writeUniqueValues(uniqueFieldValueConfig.getOutputDirectoryPath(),
                uniqueValues.keySet(),
                uniqueFieldValueConfig.getWriteBatchSize());

        System.out.printf("\nWritten %s Unique Values to file in %s seconds",
                uniqueValues.size(),
                (TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)));

    }

    static class NYCUniqueFieldValueConfig {

        private static final String USAGE_PATTERN = """
                \n\t\tUSAGE
                \t1. Number of Writer threads.
                \t2. Input file path.
                \t3. Output Directory path.
                \t4. Write Batch Size.
                \t5. Field Name
                \t6. Print Batch Size
                """;

        private final int numWriterThreads;

        private final String inputFileName;

        private final String outputDirectoryPath;

        private final int writeBatchSize;

        private final String fieldName;

        private final int printBatchSize;

        public NYCUniqueFieldValueConfig(String[] args) {
            if (args.length != 6) {
                System.out.println(USAGE_PATTERN);
                System.exit(1);
            }

            numWriterThreads = Integer.parseInt(args[0].trim());
            inputFileName = args[1];
            outputDirectoryPath = args[2];
            writeBatchSize = Integer.parseInt(args[3].trim());
            fieldName = args[4];
            printBatchSize = Integer.parseInt(args[5]);
        }

        public int getNumWriterThreads() {
            return numWriterThreads;
        }

        public String getInputFileName() {
            return inputFileName;
        }

        public String getOutputDirectoryPath() {
            return outputDirectoryPath;
        }

        public int getWriteBatchSize() {
            return writeBatchSize;
        }

        public String getFieldName() {
            return fieldName;
        }

        public int getPrintBatchSize() {
            return printBatchSize;
        }
    }

    static class WriterThread implements Runnable {

        private final long startByte;

        private final long numBytesToRead;

        private final Path inputFile;

        private final String threadName;

        private final NYCUniqueFieldValueConfig uniqueFieldValueConfig;

        private final Map<Object, Object> uniqueValues;

        private final AtomicLong lastPrintedUniqueCount;

        public WriterThread(long startByte,
                            long numBytesToRead,
                            NYCUniqueFieldValueConfig uniqueFieldValueConfig,
                            Map<Object, Object> uniqueValues,
                            AtomicLong lastPrintedUniqueCount) {
            this.startByte = startByte;
            this.numBytesToRead = numBytesToRead;
            this.inputFile = Paths.get(uniqueFieldValueConfig.getInputFileName());
            this.threadName = String.format("WriterThread-startByte-%s", startByte);
            this.uniqueFieldValueConfig = uniqueFieldValueConfig;
            this.uniqueValues = uniqueValues;
            this.lastPrintedUniqueCount = lastPrintedUniqueCount;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(threadName);
            byte[] buffer = new byte[uniqueFieldValueConfig.getWriteBatchSize()];
            try (FileInputStream inputStream = new FileInputStream(inputFile.toFile())) {

                if (startByte > 0) {
                    long skippedBytes = inputStream.skip(startByte - 1);
                    System.out.printf("\nSkipped %s bytes in thread %s\n", skippedBytes, threadName);
                } else {
                    System.out.printf("\nStarted reading in thread %s\n", threadName);
                }

                long numBytesRead = 0;

                List<String> currentLineBatch = new LinkedList<>();

                String lastPartial = "";

                while (numBytesRead < numBytesToRead) {

                    int bytesRead = inputStream.read(buffer);
                    boolean isPartial = GenerateNYCTaxiIndex.getEntries(buffer, bytesRead, currentLineBatch, lastPartial);

                    if (isPartial) {
                        lastPartial = currentLineBatch.getLast();
                        currentLineBatch.removeLast();
                    } else {
                        lastPartial = "";
                    }

                    for (String entry : currentLineBatch) {
                        recordEntry(entry);
                    }

                    // Reset for next batch
                    currentLineBatch.clear();
                    numBytesRead += bytesRead;
                }


            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.printf("\nDone reading in thread %s\n", threadName);

            }
        }

        private void recordEntry(String json) {
            TaxiData data;
            try {
                data = new TaxiData(json);
                Object fieldValue = data.getFieldFromName(uniqueFieldValueConfig.getFieldName());
                uniqueValues.putIfAbsent(fieldValue, fieldValue);
                if (uniqueValues.size() % uniqueFieldValueConfig.getPrintBatchSize() == 0 && uniqueValues.size() > lastPrintedUniqueCount.get()) {
                    System.out.printf("\nWritten %s unique values for field %s\n", uniqueValues.size(), uniqueFieldValueConfig.getFieldName());
                    lastPrintedUniqueCount.set(uniqueValues.size());
                }
                //System.out.printf("\nRead in thread %s data %s\n", threadName, data);
            } catch (JsonProcessingException e) {
                System.out.printf("\nIgnoring in thread %s for %s as it's invalid json\n", threadName, json);
            }
        }

    }

    private static void writeUniqueValues(String outFile, Set<Object> uniqueValues, int batchSize) throws IOException {

        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outFile))) {
            List<byte[]> bufferedValues = new ArrayList<>(batchSize);

            int totalBytesBuffered = 0;

            for (Object value : uniqueValues) {
                if (bufferedValues.size() == batchSize) {
                    ByteBuffer temp = ByteBuffer.wrap(new byte[totalBytesBuffered]);
                    bufferedValues.forEach(temp::put);
                    bufferedOutputStream.write(temp.array());
                    // Reset internal metadata
                    totalBytesBuffered = 0;
                    bufferedValues.clear();
                }
                byte[] valAsBytes = (value.toString() + "\n").getBytes();
                bufferedValues.add(valAsBytes);
                totalBytesBuffered += valAsBytes.length;
            }

            if (!bufferedValues.isEmpty()) {
                ByteBuffer temp = ByteBuffer.wrap(new byte[totalBytesBuffered]);
                bufferedValues.forEach(temp::put);
                bufferedOutputStream.write(temp.array());
            }
        }

    }

}

