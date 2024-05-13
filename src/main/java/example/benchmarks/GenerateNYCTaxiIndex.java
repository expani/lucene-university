package example.benchmarks;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GenerateNYCTaxiIndex {

    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        NYCIndexConfig indexConfig = new NYCIndexConfig(args);
        long bytesPerThread = (new File(indexConfig.getInputFileName()).length()) / indexConfig.getNumWriterThreads();
        long currentByteStart = 0;
        try (IndexWriter indexWriter = new IndexWriter(FSDirectory.open(Paths.get(indexConfig.getOutputDirectoryPath())), new IndexWriterConfig());
             ExecutorService threadPool = Executors.newFixedThreadPool(indexConfig.getNumWriterThreads())) {
            for (int i = 1; i <= indexConfig.getNumWriterThreads(); i++) {
                threadPool.submit(new WriterThread(currentByteStart, bytesPerThread,
                        indexConfig.getInputFileName(),
                        indexConfig.getNumDocsPerSegment(),
                        indexWriter));
                currentByteStart += bytesPerThread;
            }
        }

        System.out.printf("\nCompleted writing index in %s seconds\n", TimeUnit.SECONDS.convert((System.nanoTime() - startTime), TimeUnit.NANOSECONDS));

    }

    static class NYCIndexConfig {

        private static final String USAGE_PATTERN = """
                \n\t\tUSAGE
                \t1. Number of Writer threads.
                \t2. Input file path.
                \t3. Output Directory path.
                \t4. Number of documents per segment.
                """;

        private final int numWriterThreads;

        private final String inputFileName;

        private final String outputDirectoryPath;

        private final int numDocsPerSegment;

        public NYCIndexConfig(String[] args) {
            if (args.length != 4) {
                System.out.println(USAGE_PATTERN);
                System.exit(1);
            }

            numWriterThreads = Integer.parseInt(args[0].trim());
            inputFileName = args[1];
            outputDirectoryPath = args[2];
            numDocsPerSegment = Integer.parseInt(args[3].trim());
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

        public int getNumDocsPerSegment() {
            return numDocsPerSegment;
        }
    }

    static class WriterThread implements Runnable {

        private final long startByte;

        private final long numBytesToRead;

        private final Path inputFile;

        private final String threadName;

        private final int numDocsPerSegment;

        private int numDocsInBuffer = 0;

        private long numDocsWritten = 0;

        private final IndexWriter indexWriter;

        public WriterThread(long startByte, long numBytesToRead, String inputFile, int numDocsPerSegment, IndexWriter indexWriter) {
            this.startByte = startByte;
            this.numBytesToRead = numBytesToRead;
            this.inputFile = Paths.get(inputFile);
            this.threadName = String.format("WriterThread-startByte-%s", startByte);
            this.numDocsPerSegment = numDocsPerSegment;
            this.indexWriter = indexWriter;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(threadName);
            byte[] buffer = new byte[1024];
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
                    boolean isPartial = getEntries(buffer, bytesRead, currentLineBatch, lastPartial);

                    if (isPartial) {
                        lastPartial = currentLineBatch.getLast();
                        currentLineBatch.removeLast();
                    } else {
                        lastPartial = "";
                    }

                    for (String entry : currentLineBatch) {
                        writeEntryToIndex(entry);
                    }

                    // Reset for next batch
                    currentLineBatch.clear();
                    numBytesRead += bytesRead;
                }

                if (numDocsInBuffer > 0) {
                    System.out.printf("\nWriter thread [%s] last commit as docs in buffer reached [%s]\n", threadName, numDocsInBuffer);
                    indexWriter.commit();
                    numDocsInBuffer = 0;
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.printf("\nDone writing in thread %s and written %s docs\n", threadName, numDocsWritten);

            }
        }

        private void writeEntryToIndex(String json) {
            TaxiData data = null;
            try {
                data = new TaxiData(json);
                if (numDocsInBuffer > 0 && numDocsInBuffer >= numDocsPerSegment) {
                    System.out.printf("\nWriter thread [%s] committing as docs in buffer reached [%s]\n", threadName, numDocsInBuffer);
                    indexWriter.commit();
                    numDocsInBuffer = 0;
                }
                indexWriter.addDocument(data.toDocument());
                numDocsInBuffer++;
                numDocsWritten++;
                //System.out.printf("\nRead in thread %s data %s\n", threadName, data);
            } catch (JsonProcessingException e) {
                System.out.printf("\nIgnoring in thread %s for %s as it's invalid json\n", threadName, json);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static boolean getEntries(byte[] buffer, int bytesToRead, List<String> lines, String lastPartial) {

        StringBuilder currentLine = new StringBuilder();

        int linesRead = 0;

        for (int i = 0; i < bytesToRead; i++) {
            char currentChar = (char) buffer[i];
            if (currentChar == '\n') {
                if (linesRead == 0 && !lastPartial.trim().isEmpty()) {
                    lines.add(lastPartial + currentLine);
                } else {
                    lines.add(currentLine.toString());
                }
                currentLine.setLength(0);
                linesRead++;
            } else {
                currentLine.append(currentChar);
            }
        }

        if (!currentLine.isEmpty()) {
            lines.add(currentLine.toString());
        }

        // If a partial line was read, then return true.
        return !currentLine.isEmpty();

    }

}
