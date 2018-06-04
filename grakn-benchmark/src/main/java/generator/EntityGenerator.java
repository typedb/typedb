package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.TypeStrategy;

import java.util.stream.Stream;

public class EntityGenerator extends Generator<TypeStrategy> {
    public EntityGenerator(TypeStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        return super.generate();
    }
}
