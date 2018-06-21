package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.*;
import pdf.ConstantPDF;
import pick.StreamProviderInterface;
import strategy.AttributeOwnerTypeStrategy;
import strategy.AttributeStrategy;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class AttributeGenerator<Datatype> extends Generator<AttributeStrategy<Datatype>> {

    public AttributeGenerator(AttributeStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    @Override
    public Stream<Query> generate() {

        // TODO 2 lines common to all 3 generators
        QueryBuilder qb = this.tx.graql();
        int numInstances = this.strategy.getNumInstancesPDF().next();

        AttributeOwnerTypeStrategy attributeOwnerStrategy = this.strategy.getAttributeOwnerStrategy();
        StreamProviderInterface<ConceptId> ownerPicker = attributeOwnerStrategy.getPicker();

        StreamProviderInterface<Datatype> valuePicker = this.strategy.getPicker();
        String attributeTypeLabel = this.strategy.getTypeLabel();

        valuePicker.reset();
        ownerPicker.reset();

        ConstantPDF unityPDF = new ConstantPDF(1);

        return Stream.generate(() -> {

            Stream<ConceptId> ownerConceptIdStream = ownerPicker.getStream(unityPDF, tx);

            Optional<ConceptId> ownerConceptIdOptional = ownerConceptIdStream.findFirst();

            if (ownerConceptIdOptional.isPresent()) {
                Stream<Datatype> valueStream = valuePicker.getStream(unityPDF, tx);

                ConceptId ownerConceptId = ownerConceptIdOptional.get();
                Datatype value = valueStream.findFirst().get();

                Var c = Graql.var().asUserDefined();

                return (Query) qb.insert(c.has(attributeTypeLabel).val(value), c.id(ownerConceptId));
            } else {
                return null;
            }
        }).limit(numInstances).filter(Objects::nonNull);
    }
}
