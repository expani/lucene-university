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

        if (args.length != 3) {
            System.out.println("Usage: <FieldName> <Field Value> <index dir>");
            System.exit(1);
        }

        String fieldName = args[0];
        String fieldValue = args[1];
        String indexPath = args[2];

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            long start = System.nanoTime();
            TopDocs topDocs = searcher.search(new TermQuery(new Term(fieldName, fieldValue)), 10_000);
            double took = (System.nanoTime() - start) / 1_000_000.0;
            int maxDocId = Integer.MIN_VALUE;
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                maxDocId = Math.max(maxDocId, scoreDoc.doc);
            }
            System.out.println("Query completed with max doc id: " + maxDocId + ", took: " + took + " milliseconds");
        }

    }

}