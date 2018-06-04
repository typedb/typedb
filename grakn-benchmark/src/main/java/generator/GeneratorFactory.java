package generator;

import ai.grakn.GraknTx;
import strategy.RelationStrategy;
import strategy.TypeStrategy;

public class GeneratorFactory {

    public static EntityGenerator create(TypeStrategy st, GraknTx tx){
        return new EntityGenerator(st, tx);
    }

    public static RelationGenerator create(RelationStrategy st, GraknTx tx){
        return new RelationGenerator(st, tx);
    }

}
