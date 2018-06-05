package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.RelationshipStrategy;

import java.util.stream.Stream;

public class RelationshipGenerator extends Generator<RelationshipStrategy> {

    public RelationshipGenerator(RelationshipStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        return null;
    }
}
