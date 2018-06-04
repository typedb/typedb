package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.RelationStrategy;

import java.util.stream.Stream;

public class RelationGenerator extends Generator<RelationStrategy> {

    public RelationGenerator(RelationStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        return super.generate();
    }
}
