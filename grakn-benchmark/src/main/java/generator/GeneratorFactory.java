package generator;

import ai.grakn.GraknTx;
import storage.ConceptStore;
import strategy.*;

public class GeneratorFactory {

    public Generator create(Object typeStrategy, GraknTx tx) {
        /*

        We want to pass a structure like:
        TypeStrategy -> EntityGenerator
        RelationshipStrategy -> RelationshipGenerator
        AttributeStrategy -> AttributeGenerator

        that way when adding new generators this class doesn't need to be touched
         */

        if (typeStrategy instanceof EntityStrategy) {
            return new EntityGenerator((EntityStrategy) typeStrategy, tx);
        } else if (typeStrategy instanceof RelationshipStrategy) {
            return new RelationshipGenerator((RelationshipStrategy) typeStrategy, tx);
        } else if (typeStrategy instanceof AttributeStrategy) {
            return new AttributeGenerator((AttributeStrategy) typeStrategy, tx);
        }
        throw new RuntimeException("Couldn't find a matching Generator for this strategy");
    }

}
