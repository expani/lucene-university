package example.benchmarks;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneSimpleBenchmark {

    public static void main(String[] args) {
        BenchmarkConfig config = new BenchmarkConfig(args);
        runBenchmark(config);
    }

    private static void runBenchmark(BenchmarkConfig config) {

        try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(config.indexDir)))) {

            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            double totalTimeTaken = 0L;

            long startTime = System.nanoTime();

            long totalHits = 0L;

            for (int i = 1; i <= config.iterations; i++) {
                TopScoreDocCollector collector = TopScoreDocCollector.create(config.numHits, config.numHits);
                indexSearcher.search(new MatchAllDocsQuery(), collector);
                totalHits += collector.getTotalHits();
            }

            double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            totalTimeTaken += elapsed;

            System.out.printf("\nTotal time taken for match all docs query is %s seconds with hits %s\n", elapsed, totalHits);

            totalHits = 0L;

            startTime = System.nanoTime();

            for (int i = 1; i <= config.iterations; i++) {
                TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(config.numHits, config.numHits);
                indexSearcher.search(DoublePoint.newRangeQuery("totalAmount", 5.0, 15.0), topScoreDocCollector);
                totalHits += topScoreDocCollector.getTotalHits();
            }

            elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;

            totalTimeTaken += elapsed;

            System.out.printf("\nTotal time taken for double range query is %s seconds with hits %s\n", elapsed, totalHits);
            System.out.println("Total time taken overall is " + totalTimeTaken + " seconds");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static class BenchmarkConfig {

        private static final String USAGE = """
                \n\t USAGE
                \t1. Index directory path
                \t2. Num hits
                \t3. Num Iterations
                \n
                """;

        private final int iterations;

        private final int numHits;

        private final String indexDir;

        public BenchmarkConfig(String[] args) {
            if (args.length != 3) {
                System.out.println(USAGE);
                System.exit(1);
            }

            indexDir = args[0];
            numHits = Integer.parseInt(args[1]);
            iterations = Integer.parseInt(args[2]);
        }

    }


}
