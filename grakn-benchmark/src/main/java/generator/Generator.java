package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.TypeStrategy;

import java.util.stream.Stream;

public abstract class Generator<T extends TypeStrategy> {

    private final T strategy;
    private final GraknTx tx;

    public Generator(T strategy, GraknTx tx) {
        this.strategy = strategy;
        this.tx = tx;
    }

    public Stream<Query> generate(){
        return null;
    }
}
