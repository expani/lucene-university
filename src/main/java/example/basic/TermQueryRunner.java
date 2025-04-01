package example.basic;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class TermQueryRunner {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println("Usage: <FieldName> <Field Value> <index dir> <hits : optional> <warmup iters>");
            System.exit(1);
        }

        String fieldName = args[0];
        String fieldValue = args[1];
        String indexPath = args[2];

        int hits = args.length > 3 ? Integer.parseInt(args[3]) : 0;

        int warmupIters = args.length > 4 ? Integer.parseInt(args[4]) : 0;

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TermQuery termQuery = new TermQuery(new Term(fieldName, fieldValue));

            int blackhole = 0;

            for (int i = 1; i <= warmupIters; i++) {
                blackhole += searcher.search(termQuery, hits).scoreDocs.length;
            }

            System.out.println("Warmup done with scored docs: " + blackhole);

            long start = System.nanoTime();
            TopDocs topDocs = searcher.search(termQuery, hits);
            double took = (System.nanoTime() - start) / 1_000_000.0;
            int maxDocId = Integer.MIN_VALUE;
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                maxDocId = Math.max(maxDocId, scoreDoc.doc);
            }
            System.out.println("Query completed with max doc id: " + maxDocId + ", took: " + took + " milliseconds");
        }

    }

}