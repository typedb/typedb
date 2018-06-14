package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.TypeStrategy;

import java.util.stream.Stream;

// TODO Should generator have an interface to make it easy to pass generators of different types. This means passing a TypeStrategy as a parameter
public abstract class Generator<T extends TypeStrategy> {

    protected final T strategy;
    protected final GraknTx tx;

    public Generator(T strategy, GraknTx tx) {
        this.strategy = strategy;
        this.tx = tx;
    }

    public abstract Stream<Query> generate();

}
