package example.benchmarks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

public class DocIdsBenchmark {

    public static void main(String[] args) {
        runDocIdBenchmark(args[0], args[1], args[2], Integer.parseInt(args[3]));
    }

    public static void runDocIdBenchmark(String fieldName, String inputFile, String indexDir, int printSize) {

        try (Scanner fileReader = new Scanner(new File(inputFile));
             IndexWriter indexWriter = new IndexWriter(FSDirectory.open(Paths.get(indexDir)), new IndexWriterConfig())) {

            fileReader.useDelimiter("\n");

            int docsAdded = 0;

            while (fileReader.hasNext()) {
                String currLine = fileReader.next();
                TaxiData data = new TaxiData(currLine);
                Document document = new Document();
                document.add(new DoublePoint(fieldName, (Double) data.getFieldFromName(fieldName)));
                indexWriter.addDocument(document);
                docsAdded++;
                if (docsAdded % printSize == 0) {
                    System.out.println("Added " + docsAdded + " docs");
                }
            }

            indexWriter.commit();
            System.out.println("\nForce merging into 1 segment\n");
            indexWriter.forceMerge(1);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
