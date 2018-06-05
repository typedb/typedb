package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Match;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import strategy.EntityStrategy;

import java.util.ArrayList;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class EntityGenerator extends Generator<EntityStrategy> {
    public EntityGenerator(EntityStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {
        QueryBuilder qb = this.tx.graql();

        // TODO Can using toString be avoided? Waiting for TP task #20179
        String entityTypeName = this.strategy.getType().getLabel().getValue();
        return Stream.generate( () -> qb.insert(var("x").isa(entityTypeName)) )
                .map(q -> (Query) q)
                .limit(this.strategy.getNumInstancesPDF().next());
    }
}
