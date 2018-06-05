package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.AttributeStrategy;

import java.util.stream.Stream;

public class AttributeGenerator extends Generator<AttributeStrategy> {

    public AttributeGenerator(AttributeStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        return null;
    }
}
