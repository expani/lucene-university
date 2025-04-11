package example.basic;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class TermQueryRunner {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println("Usage: <FieldName> <Field Value> <index dir> <hits : optional> <warmupIter>");
            System.exit(1);
        }

        String fieldName = args[0];
        String fieldValue = args[1];
        String indexPath = args[2];

        int hits = args.length > 3 ? Integer.parseInt(args[3]) : 0;

        int warmupIter = args.length > 4 ? Integer.parseInt(args[4]) : 0;

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TermQuery termQuery = new TermQuery(new Term(fieldName, fieldValue));

            System.out.println("Starting warmup");
            long warmupStart = System.nanoTime();

            Collector collector = new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) {}

                        @Override
                        public void collect(int doc) {
                            throw new CollectionTerminatedException();
                        }
                    };
                }

                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.TOP_SCORES;
                }
            };

            for (int i = 1; i <= warmupIter; i++) {
                searcher.search(termQuery, collector);
            }

            System.out.println("Warmup took " + (System.nanoTime() - warmupStart) / 1_000_000.0 + " ms using collector " + collector);

            long start = System.nanoTime();
            TopScoreDocCollector topScoreDocCollector = new TopScoreDocCollectorManager(hits, hits).newCollector();
            searcher.search(termQuery, topScoreDocCollector);
            double took = (System.nanoTime() - start) / 1_000_000.0;
            System.out.println("Query completed and took: " + took + " milliseconds with total hits: " + topScoreDocCollector.getTotalHits());
        }

    }

}