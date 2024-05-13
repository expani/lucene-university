package example.benchmarks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class UniqueTermsLuceneIndex {

    public static void main(String[] args) {

        String indexDir = "/Users/anijainc/Desktop/AOS_Search/Data/NYCIndex/";
        String fieldName = "totalAmount";

        try(IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {

            IndexSearcher searcher = new IndexSearcher(indexReader);

            TopDocs topDocs = searcher.search(DoublePoint.newRangeQuery(fieldName, 9.8, 119.8), 10);

            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document document = searcher.storedFields().document(scoreDoc.doc);
                System.out.println(document.getField(fieldName));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
