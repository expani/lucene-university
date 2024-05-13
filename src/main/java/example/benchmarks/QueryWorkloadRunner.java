package example.benchmarks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class QueryWorkloadRunner {

    public static final String MULTI_QUERY_CONFIG_DELIM = "###";

    public static final String PER_QUERY_CONFIG_DELIM = "::";

    public static final String PER_QUERY_FIELDS_CONFIG_DELIM = ",";

    public static final String THREAD_NAME_DELIMITER = ":";

    private static final TimeUnit SUMMARY_UNIT = TimeUnit.SECONDS;

    private static final String MAIN_THREAD_PREFIX = "MainThread";

    private static final String LOAD_GEN_THREAD_PREFIX = "LoadGenThread";

    private static final Map<TimeUnit, Double> DIVISOR_FROM_NANOS = Map.of(
            TimeUnit.MICROSECONDS, 1000.0,
            TimeUnit.MILLISECONDS, 1_000_000.0,
            TimeUnit.SECONDS, 1_000_000_000.0
    );

    public static void main(String[] args) throws Exception {
        QueryWorkloadRunner runner = new QueryWorkloadRunner();
        runner.runWorkload(
                new WorkloadConfig(Arrays.copyOfRange(args, 0, args.length - 1)),
                args[args.length - 1]
        );
    }

    public void runWorkload(WorkloadConfig workloadConfig, String memoryFillerArgs) throws Exception {

        WorkloadStats workloadStats = new WorkloadStats(THREAD_NAME_DELIMITER, workloadConfig);

        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(workloadConfig.indexDirPath)));
             ExecutorService mainThreadPool = Executors.newFixedThreadPool(workloadConfig.numMainThreads);
             ExecutorService loadGenThreadPool = Executors.newFixedThreadPool(workloadConfig.numLoadGenThreads);
             ExecutorService memoryFillerPool = Executors.newFixedThreadPool(1)) {

            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            executeWarmupQueries(indexSearcher, workloadConfig.mainQueryConfig.queries, workloadConfig.warmupQueries);

            List<Future<?>> mainThreadFutures = new ArrayList<>();

            AtomicBoolean stop = new AtomicBoolean(false);

            for (int i = 1; i <= workloadConfig.numMainThreads; i++) {
                mainThreadFutures.add(mainThreadPool.submit(new SearchWorker(MAIN_THREAD_PREFIX + THREAD_NAME_DELIMITER + i,
                        workloadConfig.mainQueryConfig.numIterations,
                        Collections.singletonList(workloadConfig.mainQueryConfig),
                        indexSearcher,
                        workloadStats,
                        stop
                )));
            }

            for (int i = 1; i <= workloadConfig.numLoadGenThreads; i++) {
                loadGenThreadPool.submit(new SearchWorker(LOAD_GEN_THREAD_PREFIX + THREAD_NAME_DELIMITER + i,
                        workloadConfig.loadGenQueryConfigs.get(0).numIterations,
                        workloadConfig.loadGenQueryConfigs,
                        indexSearcher,
                        workloadStats,
                        stop
                ));
            }

            if (!memoryFillerArgs.trim().equalsIgnoreCase("false")) {
                memoryFillerPool.submit(new MemoryFiller(memoryFillerArgs, stop));
            }

            mainThreadFutures.forEach(x -> {
                try {
                    x.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            System.out.println("Main Thread's have completed execution. Signalling load gen threads to stop.....");

            stop.set(true);

        }

        System.out.println(workloadStats.getSummary());
        System.out.printf("\nWorkload execution started at [%s] and completed at [%s]\n", startTime, new Timestamp(System.currentTimeMillis()));

    }

    static class SearchWorker implements Runnable {

        private final String id;

        private final int numIterations;
        private final List<PerQueryConfig> perQueryConfigs;

        private final IndexSearcher indexSearcher;

        private final WorkloadStats workloadStats;

        private final AtomicBoolean stop;

        public SearchWorker(String id,
                            int numIterations,
                            List<PerQueryConfig> perQueryConfigs,
                            IndexSearcher indexSearcher,
                            WorkloadStats workloadStats,
                            AtomicBoolean stop) {
            this.id = id;
            this.numIterations = numIterations;
            this.perQueryConfigs = perQueryConfigs;
            this.indexSearcher = indexSearcher;
            this.workloadStats = workloadStats;
            this.stop = stop;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(id);
            long fieldValueCount = 0;
            long totalHits = 0;
            for (int i = 1; i <= numIterations; i++) {
                if (stop.get()) break;
                System.out.printf("\nRunning iteration [%s] in thread [%s] at %s\n", i, id, new Timestamp(System.currentTimeMillis()));
                for (PerQueryConfig perQueryConfig : perQueryConfigs) {
                    if (stop.get()) break;
                    for (Query query : perQueryConfig.queries) {
                        if (stop.get()) break;
//                        if (Thread.currentThread().getName().startsWith(MAIN_THREAD_PREFIX)) {
//                            System.out.printf("\nRunning query %s in thread %s \n", query, id);
//                        }
                        try {
                            //System.out.printf("\nRunning query %s in thread %s\n", query.toString(), id);
                            TopScoreDocCollector collector = TopScoreDocCollector.create(1000, 1000);
                            workloadStats.recordQueryStart(id);
                            indexSearcher.search(query, collector);
                            workloadStats.recordQueryEnd(id);
                            totalHits += collector.getTotalHits();
                            workloadStats.recordDocFetchStart(id);
                            if (!perQueryConfig.fieldsToFetch.isEmpty()) {
                                List<String> currentQueryFieldValues = new LinkedList<>();
                                for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs) {
                                    Document document = indexSearcher.storedFields().document(scoreDoc.doc);
                                    for (String fieldName : perQueryConfig.fieldsToFetch) {
//                                        System.out.printf("\nField name %s having value %s in thread %s\n",
//                                                fieldName,
//                                                document.getField(fieldName),
//                                                id);
                                        IndexableField field = document.getField(fieldName);
                                        if (field != null && field.stringValue() != null) {
                                            currentQueryFieldValues.add(field.stringValue());
                                        }
                                    }
                                }
                                fieldValueCount += currentQueryFieldValues.size();
                            }
                            workloadStats.recordDocFetchEnd(id);
                        } catch (IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                }
                System.out.printf("\nCompleted iteration [%s] in thread [%s] at %s\n",
                        i, id, new Timestamp(System.currentTimeMillis()));
            }
            System.out.printf("\nTotal Field Value Count in thread %s is %s and total hits is %s for query type %s\n",
                    id, fieldValueCount, totalHits,
                    perQueryConfigs.stream().map(x -> x.queryAndDataType.toString()).reduce((x, y) -> x + "," + y).get());
        }


    }

    static interface QueryGenerator {
        public List<Query> generateQuery(String fieldName, List<String> inputs);
    }

    static class WorkloadConfig {

        private final PerQueryConfig mainQueryConfig;

        private final List<PerQueryConfig> loadGenQueryConfigs;

        private final int numMainThreads;

        private final int numLoadGenThreads;

        private final int warmupQueries;

        private final String indexDirPath;

        // [MainPerQueryConfig]<<>>[LoadPerQueryConfig1]<<>>[LoadPerQueryConfig2]....
        public WorkloadConfig(String[] args) throws IOException {
            this.numMainThreads = Integer.parseInt(args[0]);
            this.numLoadGenThreads = Integer.parseInt(args[1]);
            this.warmupQueries = Integer.parseInt(args[2]);
            this.indexDirPath = args[3];
            String[] queryConfigSplits = args[4].split(MULTI_QUERY_CONFIG_DELIM);
            mainQueryConfig = new PerQueryConfig(queryConfigSplits[0]);
            loadGenQueryConfigs = new ArrayList<>(queryConfigSplits.length - 1);
            for (int i = 1; i < queryConfigSplits.length; i++) {
                loadGenQueryConfigs.add(new PerQueryConfig(queryConfigSplits[i]));
            }
        }

    }

    static class PerQueryConfig {

        private final QueryAndDataType queryAndDataType;

        private final String fieldName;

        private final List<Query> queries;

        private final List<String> fieldsToFetch;

        private final int numHits;

        private final int numIterations;

        // LONG_RANGE::<field_name>::numHits::<absolute path to input file>::<Comma Separated fields to fetch after search>
        public PerQueryConfig(String configAsString) throws IOException {
            String[] mainSplits = configAsString.split(PER_QUERY_CONFIG_DELIM);

            queryAndDataType = QueryAndDataType.valueOf(mainSplits[0].toUpperCase());
            fieldName = mainSplits[1].trim();
            numHits = Integer.parseInt(mainSplits[2].trim());
            numIterations = Integer.parseInt(mainSplits[3].trim());
            queries = queryAndDataType.generateQuery(fieldName, Files.readAllLines(Paths.get(mainSplits[4])));
            if (mainSplits.length >= 6) {
                fieldsToFetch = Arrays.stream(mainSplits[5].split(PER_QUERY_FIELDS_CONFIG_DELIM)).collect(Collectors.toList());
            } else {
                fieldsToFetch = Collections.emptyList();
            }

        }

    }


    static enum QueryAndDataType implements QueryGenerator {

        LONG_RANGE {
            @Override
            public List<Query> generateQuery(String fieldName, List<String> inputs) {
                return inputs.stream().map(x ->
                        LongPoint.newRangeQuery(fieldName,
                                Long.parseLong(x),
                                Long.parseLong(x) + 100L)
                ).collect(Collectors.toList());
            }
        },
        DOUBLE_RANGE {
            @Override
            public List<Query> generateQuery(String fieldName, List<String> inputs) {
                return inputs.stream().map(x ->
                        DoublePoint.newRangeQuery(fieldName,
                                Double.parseDouble(x),
                                Double.parseDouble(x) + 5000.0d)
                ).collect(Collectors.toList());
            }
        },
        TEXT_TERM {
            @Override
            public List<Query> generateQuery(String fieldName, List<String> inputs) {
                return inputs.stream().map(x ->
                        new TermQuery(new Term(fieldName, x))
                ).collect(Collectors.toList());
            }
        },

        NO_MATCH {
            @Override
            public List<Query> generateQuery(String fieldName, List<String> inputs) {
                return inputs.stream().map(x ->
                        new MatchNoDocsQuery()
                ).collect(Collectors.toList());
            }
        },

        FUZZY_TERM {
            @Override
            public List<Query> generateQuery(String fieldName, List<String> inputs) {
                return inputs.stream().map(x ->
                        new FuzzyQuery(new Term(fieldName, x))
                ).collect(Collectors.toList());
            }
        };

    }

    static class WorkloadStats {

        private final String keyGroupDelimiter;

        // Thread Name -> ( Total Search Query Time, Total Doc fetch time )
        private final Map<String, Tuple<Long, Long>> groupToKeyStatsFinalMapping;

        private final Map<String, Tuple<Long, Long>> groupToKeyStatsCurrentMapping;

        private final WorkloadConfig workloadConfig;

        public WorkloadStats(String keyGroupDelimiter, WorkloadConfig workloadConfig) {
            this.keyGroupDelimiter = keyGroupDelimiter;
            groupToKeyStatsFinalMapping = new HashMap<>();
            groupToKeyStatsCurrentMapping = new HashMap<>();
            this.workloadConfig = workloadConfig;
        }

        public void recordQueryStart(String key) {
            long startTime = System.nanoTime();
            synchronized (this) {
                Tuple<Long, Long> currentMetrics = groupToKeyStatsCurrentMapping.getOrDefault(key, new Tuple<>());
                if (currentMetrics.firstField != null) {
                    throw new IllegalStateException("Invalid recordQueryStart for " + key);
                }
                currentMetrics.firstField = startTime;
                groupToKeyStatsCurrentMapping.put(key, currentMetrics);
            }
        }

        public void recordQueryEnd(String key) {
            long endTime = System.nanoTime();
            synchronized (this) {
                Tuple<Long, Long> currentMetrics = groupToKeyStatsCurrentMapping.getOrDefault(key, new Tuple<>());
                if (currentMetrics.firstField == null) {
                    throw new IllegalStateException("Invalid recordQueryEnd for " + key);
                }
                Tuple<Long, Long> finalMetrics = groupToKeyStatsFinalMapping.getOrDefault(key, new Tuple<>());
                if (finalMetrics.firstField == null) {
                    finalMetrics.firstField = 0L;
                }
                finalMetrics.firstField += (endTime - currentMetrics.firstField);
                currentMetrics.firstField = null;
                groupToKeyStatsCurrentMapping.put(key, currentMetrics);
                groupToKeyStatsFinalMapping.put(key, finalMetrics);
            }
        }

        public void recordDocFetchStart(String key) {
            long startTime = System.nanoTime();
            synchronized (this) {
                Tuple<Long, Long> currentMetrics = groupToKeyStatsCurrentMapping.getOrDefault(key, new Tuple<>());
                if (currentMetrics.secondField != null) {
                    throw new IllegalStateException("Invalid recordDocFetchStart for " + key);
                }
                currentMetrics.secondField = startTime;
                groupToKeyStatsCurrentMapping.put(key, currentMetrics);
            }
        }

        public void recordDocFetchEnd(String key) {
            long endTime = System.nanoTime();
            synchronized (this) {
                Tuple<Long, Long> currentMetrics = groupToKeyStatsCurrentMapping.getOrDefault(key, new Tuple<>());
                if (currentMetrics.secondField == null) {
                    throw new IllegalStateException("Invalid recordDocFetchEnd for " + key);
                }
                Tuple<Long, Long> finalMetrics = groupToKeyStatsFinalMapping.getOrDefault(key, new Tuple<>());
                if (finalMetrics.secondField == null) {
                    finalMetrics.secondField = 0L;
                }
                finalMetrics.secondField += (endTime - currentMetrics.secondField);
                currentMetrics.secondField = null;
                groupToKeyStatsCurrentMapping.put(key, currentMetrics);
                groupToKeyStatsFinalMapping.put(key, finalMetrics);
            }
        }

        public String getSummary() {

            System.out.println(groupToKeyStatsFinalMapping);

            int mainThreadTotalQueriesExecuted = workloadConfig.mainQueryConfig.queries.size() * workloadConfig.mainQueryConfig.numIterations;

            double searchQPS = groupToKeyStatsFinalMapping.entrySet().stream().
                    filter(kv -> kv.getKey().startsWith(MAIN_THREAD_PREFIX)).
                    map(kv -> mainThreadTotalQueriesExecuted / (kv.getValue().firstField / DIVISOR_FROM_NANOS.get(SUMMARY_UNIT))). // QPS Per Thread
                            reduce(Double::sum).get();

            double averageDocFetchTime = (groupToKeyStatsFinalMapping.entrySet().stream().
                    filter(kv -> kv.getKey().startsWith(MAIN_THREAD_PREFIX)).
                    map(kv -> kv.getValue().secondField).
                    reduce(Long::sum).get() / DIVISOR_FROM_NANOS.get(SUMMARY_UNIT)) / workloadConfig.numMainThreads;

            StringBuilder summary = new StringBuilder();

            summary.append("\nSummary of Query Workload Runner for index directory ");
            summary.append(workloadConfig.indexDirPath);
            summary.append(" and main query field [");
            summary.append(workloadConfig.mainQueryConfig.fieldName);
            summary.append("] and query type [");
            summary.append(workloadConfig.mainQueryConfig.queryAndDataType);
            summary.append("] and Load Gen Query Types [");
            summary.append(workloadConfig.loadGenQueryConfigs.stream().map(x -> x.queryAndDataType.toString()).reduce((x, y) -> x + "," + y).get());
            summary.append("] and Load Gen Query Fields [");
            summary.append(workloadConfig.loadGenQueryConfigs.stream().map(x -> x.fieldName).reduce((x, y) -> x + "," + y).get());
            summary.append("] is as follows : \n");
            summary.append(" Total Queries executed per thread = ");
            summary.append(mainThreadTotalQueriesExecuted);
            summary.append("\nSearchQPS = ");
            summary.append(searchQPS);
            summary.append(" queries/");
            summary.append(SUMMARY_UNIT);
            summary.append("\nAverage Doc Fetch Time = ");
            summary.append(averageDocFetchTime);
            summary.append(' ');
            summary.append(SUMMARY_UNIT);

            return summary.toString();

        }

    }

    static class MemoryFiller implements Runnable {

        private final int maxMemoryBytes;

        private final List<byte[]> shortLivedObjects = new LinkedList<>();

        private final List<byte[]> longLivedObjects = new LinkedList<>();

        private final AtomicBoolean stop;

        private final int bytesPerEntry;

        private final int longLivedObjectCycle;

        private int currentCycleNumber = 0;

        public MemoryFiller(String args, AtomicBoolean stop) {
            System.out.println("Memory Filler Constructor");
            this.stop = stop;
            String[] splits = args.split(PER_QUERY_CONFIG_DELIM);
            this.maxMemoryBytes = Integer.parseInt(splits[0]);
            this.bytesPerEntry = Integer.parseInt(splits[1]);
            this.longLivedObjectCycle = Integer.parseInt(splits[2]);
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
                if (currentCycleNumber % longLivedObjectCycle == 0) {
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

    private static void executeWarmupQueries(IndexSearcher indexSearcher, List<Query> queries, int numWarmupQueries) throws IOException {

        assert numWarmupQueries <= queries.size();

        int queriesExecuted = 0;

        System.out.printf("\nExecuting %s warmup queries at %s\n", numWarmupQueries, new Timestamp(System.currentTimeMillis()));

        for (Query query : queries) {
            indexSearcher.search(query, 10);
            queriesExecuted++;
            if (queriesExecuted == numWarmupQueries) {
                break;
            }
        }

        System.out.printf("\n%s warmup queries execution completed at %s\n", queriesExecuted, new Timestamp(System.currentTimeMillis()));

    }

}
