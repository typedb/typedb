package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import strategy.EntityStrategy;

import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class EntityGenerator extends Generator<EntityStrategy> {
    public EntityGenerator(EntityStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
//    public Stream<Query> generate() {
    public Stream<Query> generate() {
        QueryBuilder qb = this.tx.graql();

        // TODO Can using toString be avoided? Waiting for TP task #20179
//        String entityTypeName = this.strategy.getType().getLabel().getValue();

        String typeLabel = this.strategy.getTypeLabel();
        Query query = qb.insert(var("x").isa(typeLabel));

        int numInstances = this.strategy.getNumInstancesPDF().next();

//        Stream<Query> stream = Stream<Query>();
        Stream<Query> stream = Stream.generate(() -> query)
//                .map(q -> (Query) q)
                .limit(numInstances);
        return stream;
    }
}