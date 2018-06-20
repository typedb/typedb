package strategy;

import ai.grakn.concept.AttributeType;
import pdf.PDF;
import pick.StreamProviderInterface;


public class AttributeStrategy<Datatype> extends TypeStrategy<AttributeType> implements HasPicker{

    private PickableCollection<AttributeOwnerTypeStrategy> attributeOwnerStrategies = null;
    private final StreamProviderInterface valuePicker;
    private AttributeOwnerTypeStrategy attributeOwnerStrategy = null;

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             PickableCollection<AttributeOwnerTypeStrategy> attributeOwnerStrategies,
                             StreamProviderInterface valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategies = attributeOwnerStrategies;
        this.valuePicker = valuePicker;
    }

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             AttributeOwnerTypeStrategy attributeOwnerStrategy,
                             StreamProviderInterface valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategy = attributeOwnerStrategy;
        this.valuePicker = valuePicker;
    }

    public AttributeOwnerTypeStrategy getAttributeOwnerStrategy() {
        if (this.attributeOwnerStrategy != null) {
            return this.attributeOwnerStrategy;
        } else if (this.attributeOwnerStrategies != null) {
            return this.attributeOwnerStrategies.next();
        }
        throw new UnsupportedOperationException("AttributeStrategy must have either one owner or multiple possible owners");
    }

    @Override
    public StreamProviderInterface getConceptPicker() {
        return this.valuePicker;
    }
}