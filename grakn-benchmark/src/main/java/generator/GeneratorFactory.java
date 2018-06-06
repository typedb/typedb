package generator;

import ai.grakn.GraknTx;
import strategy.*;

public class GeneratorFactory {

//    public static EntityGenerator create(EntityStrategy st, GraknTx tx) {
//        return new EntityGenerator(st, tx);
//    };
//
//    public static RelationshipGenerator create(RelationshipStrategy st, GraknTx tx) {
//        return new RelationshipGenerator(st, tx);
//    };
//
//    public static AttributeGenerator create(AttributeStrategy st, GraknTx tx) {
//        return new AttributeGenerator(st, tx);
//    };

//    public AttributeGenerator create(Strategy st, GraknTx tx) {
////        return new AttributeGenerator(st, tx);
//        return null;
//    }

//    public <G extends Generator> G create(TypeStrategy typeStrategy, GraknTx tx) {
//        return null;
//    };

//    public <G extends Generator> G build(FrequencyOptionCollection collection, GraknTx tx){
//
//        return null;
//    };
//
//    public <G extends Generator> G create(Object typeStrategy, GraknTx tx) {




//    public Generator create(TypeStrategy typeStrategy, GraknTx tx) {
//
//        if (typeStrategy instanceof EntityStrategy){
//            return new EntityGenerator((EntityStrategy) typeStrategy, tx);
//        } else if (typeStrategy instanceof RelationshipStrategy){
//            return new RelationshipGenerator((RelationshipStrategy) typeStrategy, tx);
//        } else if (typeStrategy instanceof AttributeStrategy){
//            return new AttributeGenerator((AttributeStrategy) typeStrategy, tx);
//        }
//        throw new RuntimeException("Couldn't find a matching Generator for this strategy");
//    }


    public EntityGenerator create(TypeStrategy typeStrategy, GraknTx tx) {
        return new EntityGenerator((EntityStrategy) typeStrategy, tx);
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