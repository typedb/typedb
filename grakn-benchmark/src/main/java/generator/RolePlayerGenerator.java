package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.RolePlayerTypeStrategy;

import java.util.stream.Stream;

public class RolePlayerGenerator extends Generator<RolePlayerTypeStrategy> {

    public RolePlayerGenerator(RolePlayerTypeStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        return null;
    }
}
