package example.benchmarks;

import java.util.concurrent.TimeUnit;

public class Tuple<A, B> {

    A firstField;

    B secondField;

    @Override
    public String toString() {
        return "Tuple{" +
                "firstField=" + firstField +
                ", secondField=" + secondField +
                '}';
    }

}
