package strategy;

import ai.grakn.concept.EntityType;
import pdf.PDF;

public class EntityStrategy extends TypeStrategy<EntityType> {
    public EntityStrategy(EntityType type, PDF numInstancesPDF) {
        super(type, numInstancesPDF);
    }
}
