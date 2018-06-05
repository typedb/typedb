package generator;

import ai.grakn.GraknTx;
import strategy.AttributeStrategy;
import strategy.EntityStrategy;
import strategy.RelationshipStrategy;

public class GeneratorFactory {

    public static EntityGenerator create(EntityStrategy st, GraknTx tx){
        return new EntityGenerator(st, tx);
    }

    public static RelationshipGenerator create(RelationshipStrategy st, GraknTx tx){
        return new RelationshipGenerator(st, tx);
    }

    public static AttributeGenerator create(AttributeStrategy st, GraknTx tx){
        return new AttributeGenerator(st, tx);
    }

    public Object create(Object st, GraknTx tx) {
        return null;
    }
}


/*

We want to pass a structure like:
TypeStrategy -> EntityGenerator
RelationshipStrategy -> RelationshipGenerator
AttributeStrategy -> AttributeGenerator

 */


//public class GeneratorFactory <S extends Strategy, G extends Generator> {
//    private final Map<S, G> generatorsForStrategies;
//
//    private final Map<S, G> map = ImmutableMap.of(
//            TypeStrategy, EntityGenerator,
//            RelationshipStrategy, RelationshipGenerator
//    )
//    public GeneratorFactory(Map<S, G> generatorsForStrategies) {
//        this.generatorsForStrategies = generatorsForStrategies;
//    }
//
//    public G create(S strategy){
//        return this.generatorsForStrategies.get(strategy);
//    }
//}