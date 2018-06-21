package strategy;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Type;
import pick.StreamProviderInterface;

// TODO implement a base interface of TypeStrategyInterface?
public class AttributeOwnerTypeStrategy implements HasPicker {
    private final Type type;
    private StreamProviderInterface<ConceptId> conceptIdPicker;

    public AttributeOwnerTypeStrategy(Type type, StreamProviderInterface<ConceptId> conceptIdPicker) {
        this.type = type;
        this.conceptIdPicker = conceptIdPicker;
    }

    @Override
    public StreamProviderInterface getPicker() {
        return this.conceptIdPicker;
    }
}
