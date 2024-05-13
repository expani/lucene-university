package example.benchmarks;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class PointRangeThroughputMeasurement {
    private static final String TIMESTAMP_FIELD = "timestamp";

    private static BufferedReader openInputFile(Path inputPath) throws IOException {
        InputStream inputStream = Files.newInputStream(inputPath);
        if (inputPath.toString().endsWith(".gz")) {
            inputStream = new GZIPInputStream(inputStream);
        }
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    private static class QueryStats {

        private final AtomicLong totalTimeTakenNanos;

        private final AtomicLong totalQueriesExecuted;

        private final Map<String, Long> queryKeyStartTime;

        private final Map<Integer, Long> perThreadTotalTime;

        private final Map<Integer, Long> perThreadTotalQueries;

        private final int numThreads;

        public QueryStats(int numQueries, int numThreads) {
            totalQueriesExecuted = new AtomicLong(0);
            totalTimeTakenNanos = new AtomicLong(0);
            queryKeyStartTime = new ConcurrentHashMap<>(numQueries);
            perThreadTotalTime = new ConcurrentHashMap<>(numThreads);
            perThreadTotalQueries = new ConcurrentHashMap<>(numThreads);
            this.numThreads = numThreads;
        }

        public void recordQueryStart(int workerId, int queryId) {
            long currentTime = System.nanoTime();
            String queryKey = getQueryKey(workerId, queryId);
            if (queryKeyStartTime.containsKey(queryKey)) {
                throw new IllegalStateException("Query key already exists " + queryKey);
            }
            queryKeyStartTime.put(queryKey, currentTime);
        }

        public void recordQueryComplete(int workerId, int queryId) {
            long endTime = System.nanoTime();
            String queryKey = getQueryKey(workerId, queryId);
            Long startTime = queryKeyStartTime.get(queryKey);
            if (startTime == null) {
                throw new IllegalStateException("Query key doesn't exist " + queryKey);
            }
            totalQueriesExecuted.addAndGet(1);
            totalTimeTakenNanos.addAndGet(endTime - startTime);
            //System.out.printf("\nQuery %s took %s ns\n", queryKey, (endTime - startTime));
            queryKeyStartTime.remove(queryKey);
            perThreadTotalTime.put(workerId, perThreadTotalTime.getOrDefault(workerId, 0L) + (endTime - startTime));
            perThreadTotalQueries.put(workerId, perThreadTotalQueries.getOrDefault(workerId, 0L) + 1);
        }

        public double getQps() {
            return (totalQueriesExecuted.get() / (totalTimeTakenNanos.get() / 1_000_000_000.0)) * numThreads;
        }

        public String getSummary() {
            return totalQueriesExecuted.get() +
                    " total queries executed and cumulative time taken by all threads is " +
                    (totalTimeTakenNanos.get() / 1_000_000_000.0) +
                    " seconds with an overall QPS of " +
                    getQps() +
                    " queries/sec";
        }

        public String getPerThreadSummary() {
            System.out.println(perThreadTotalQueries);
            System.out.println(perThreadTotalTime);
            return String.valueOf(perThreadTotalQueries.entrySet().stream().map(
                    kv -> kv.getValue() / (perThreadTotalTime.get(kv.getKey()) / 1_000_000_000.0)
            ).reduce(Double::sum).get());
        }

        private String getQueryKey(int workerId, int queryId) {
            return workerId + ":" + queryId;
        }

    }

    private static class PoolFiller implements Runnable {
        private final int[] finalQps;
        private final AtomicInteger pool;
        private final CountDownLatch stopLatch;
        private int currentQps;
        private int deltaQps;
        private int foundTarget = 0;


        public PoolFiller(int[] finalQps, AtomicInteger pool, CountDownLatch stopLatch, int startQPS, int startDelta) {
            this.finalQps = finalQps;
            this.pool = pool;
            this.stopLatch = stopLatch;
            this.currentQps = startQPS;
            this.deltaQps = startDelta;
        }

        @Override
        public void run() {
            int poolSize = pool.get();
            if (poolSize <= 0) {
                // Pool was drained, increase target QPS
                currentQps += deltaQps;
                System.out.println("Increasing QPS to " + currentQps);
                pool.addAndGet(currentQps);
            } else {
                // Pool was not drained, decrease target QPS and reduce delta
                deltaQps /= 2;
                if (deltaQps == 0) {
                    if (++foundTarget >= 5) {
                        finalQps[0] = currentQps;
                        stopLatch.countDown();
                    }
                    deltaQps = 1;
                }
                currentQps -= deltaQps;
                System.out.println("Decreasing QPS to " + currentQps);
                pool.set(currentQps);
            }
        }
    }

    private static class MemoryFiller implements Runnable {

        private final int maxMemoryBytes;

        private final List<byte[]> shortLivedObjects = new LinkedList<>();

        private final List<byte[]> longLivedObjects = new LinkedList<>();

        private final AtomicBoolean stop;

        private final int bytesPerEntry;

        private int currentCycleNumber = 0;

        public MemoryFiller(int maxMemoryBytes, AtomicBoolean stop, int bytesPerEntry) {
            System.out.println("Memory Filler Constructor");
            this.maxMemoryBytes = maxMemoryBytes;
            this.stop = stop;
            this.bytesPerEntry = bytesPerEntry;
        }

        @Override
        public void run() {
            System.out.println("Memory Filler Run");
            while (!stop.get()) {
                currentCycleNumber++;
                int numEntries = maxMemoryBytes / bytesPerEntry;
                if (!shortLivedObjects.isEmpty()) {
                    shortLivedObjects.clear();
                } else {
                    for (int i = 0; i < numEntries; i++) {
                        shortLivedObjects.add(new byte[bytesPerEntry]);
                    }
                }
                if (currentCycleNumber % 5000 == 0) {
                    longLivedObjects.clear();
                    System.out.println("Allocating old gen");
                    for (int i = 0; i < numEntries / 4; i++) {
                        longLivedObjects.add(new byte[bytesPerEntry]);
                    }
                }
            }
            System.out.println("Total cycles " + currentCycleNumber);
        }

    }

    private static class Worker implements Runnable {
        private final Query[] queries;
        private final IndexSearcher searcher;
        private final AtomicInteger pool;
        private final AtomicBoolean stop;
        private int pos;

        private final QueryStats queryStats;

        private final int workerId;

        public Worker(int workerId, Query[] queries, IndexSearcher searcher, int startPos, AtomicInteger pool, AtomicBoolean stop, QueryStats queryStats) {
            this.workerId = workerId;
            this.queries = queries;
            this.searcher = searcher;
            this.pos = startPos;
            this.pool = pool;
            this.stop = stop;
            this.queryStats = queryStats;
        }

        @Override
        public void run() {
            while (!stop.get()) {
                boolean wait = true;
                if (pool.get() > 0) {
                    int val = pool.decrementAndGet();
                    if (val >= 0) {
                        //TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(10, Integer.MAX_VALUE);
                        try {
                            queryStats.recordQueryStart(workerId, pos);
                            searcher.search(queries[pos], 10); // Thing to be measured
                            queryStats.recordQueryComplete(workerId, pos);
                            Timestamp end = new Timestamp(System.currentTimeMillis());
                            //System.out.printf("\n%s Executed query %s\n", end, pos);
                            pos++;
                        } catch (IOException e) {
                            System.err.println(e.getMessage());
                        }
                        if (pos >= queries.length) {
                            pos = 0;
                        }
                        wait = false; // If we ran, then try to run again immediately
                    } else {
                        pool.incrementAndGet(); // Put back the token we took, since we're not using it
                    }
                }
                if (wait) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // Assume we won't get interrupted
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final int workerCount = Integer.parseInt(args[1]);
        Path tmpDir = Files.createTempDirectory(PointRangeBenchmark.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir)) {
            Query[] queries;
            try (BufferedReader bufferedReader = openInputFile(Path.of(args[0]))) {
                try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                    int numDocs = Integer.parseInt(bufferedReader.readLine());
                    for (int i = 0; i < numDocs; i++) {
                        writer.addDocument(Collections.singleton(new LongPoint(TIMESTAMP_FIELD,
                                Long.parseLong(bufferedReader.readLine()) + GenerateNumericDataPoints.BASE_TIMESTAMP)));
                        if (i > 0 && i % 1_000_000 == 0) {
                            writer.flush();
                        }
                    }
                }
                int numQueries = Integer.parseInt(bufferedReader.readLine());
                queries = new Query[numQueries];
                for (int i = 0; i < numQueries; i++) {
                    String line = bufferedReader.readLine();
                    int commaPos = line.indexOf(',');
                    long start = Long.parseLong(line.substring(0, commaPos)) + GenerateNumericDataPoints.BASE_TIMESTAMP;
                    long end = Long.parseLong(line.substring(commaPos + 1)) + GenerateNumericDataPoints.BASE_TIMESTAMP;
                    queries[i] = LongPoint.newRangeQuery(TIMESTAMP_FIELD, start, end);
                }
            }

            System.out.println("Finished writing index to " + tmpDir);

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Do 10,000 queries to warm up
                int warmupCount = Math.min(10000, queries.length);
                QueryStats queryStats = new QueryStats(queries.length, workerCount);
                for (int i = 0; i < warmupCount; i++) {
                    searcher.search(queries[i], 10);
                }
                int[] finalQps = new int[1];
                Timestamp startTimestamp = new Timestamp(System.currentTimeMillis());
                try (ExecutorService executor = Executors.newFixedThreadPool(workerCount)) {

                    AtomicInteger pool = new AtomicInteger(0);
                    AtomicBoolean stop = new AtomicBoolean(false);

                    int queryOffset = queries.length / workerCount;
                    for (int i = 0; i < workerCount; i++) {
                        executor.execute(new Worker(i + 1, queries, searcher, queryOffset * i, pool, stop, queryStats));
                    }
                    CountDownLatch stopLatch = new CountDownLatch(1);
                    try (ExecutorService scheduler2 = Executors.newFixedThreadPool(1);
                         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
                        int maxMemoryBytes = Integer.parseInt(args[2]);
                        int bytesPerEntry = Integer.parseInt(args[3]);
                        scheduler2.submit(new MemoryFiller(maxMemoryBytes, stop, bytesPerEntry));
                        scheduler.scheduleAtFixedRate(new PoolFiller(finalQps, pool, stopLatch, 500, 500), 0, 1, TimeUnit.SECONDS);
                        stopLatch.await();
                        stop.set(true); // Terminate the workers
                    }
                }
                System.out.printf("\nEndMain QueryAfterWarmup Start %s End %s",
                        startTimestamp,
                        new Timestamp(System.currentTimeMillis()));
                System.out.println("\nFinal QPS: " + finalQps[0]);
                System.out.println(queryStats.getSummary());
                System.out.println(queryStats.getPerThreadSummary());
            }
        } finally {
            try {
                try (Stream<Path> walk = Files.walk(tmpDir)) {
                    walk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }
            } catch (IOException e) {
               e.printStackTrace();
            }
        }
    }
}
