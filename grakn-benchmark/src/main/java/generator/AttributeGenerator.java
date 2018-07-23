/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import pdf.ConstantPDF;
import pick.StreamProviderInterface;
import strategy.AttributeOwnerTypeStrategy;
import strategy.AttributeStrategy;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;


/**
 * @param <OwnerDatatype>
 * @param <ValueDatatype>
 */
public class AttributeGenerator<OwnerDatatype, ValueDatatype> extends Generator<AttributeStrategy<OwnerDatatype, ValueDatatype>> {

    /**
     * @param strategy
     * @param tx
     */
    public AttributeGenerator(AttributeStrategy<OwnerDatatype, ValueDatatype> strategy, GraknTx tx) {
        super(strategy, tx);
    }

    /**
     * @return
     */
    @Override
    public Stream<Query> generate() {

        // TODO 2 lines common to all 3 generators
        QueryBuilder qb = this.tx.graql();
        int numInstances = this.strategy.getNumInstancesPDF().next();

        AttributeOwnerTypeStrategy<OwnerDatatype> attributeOwnerStrategy = this.strategy.getAttributeOwnerStrategy();
        StreamProviderInterface<OwnerDatatype> ownerPicker = attributeOwnerStrategy.getPicker();

        StreamProviderInterface<ValueDatatype> valuePicker = this.strategy.getPicker();
        String attributeTypeLabel = this.strategy.getTypeLabel();

        valuePicker.reset();
        ownerPicker.reset();

        ConstantPDF unityPDF = new ConstantPDF(1);

        return Stream.generate(() -> {

            // Owner stream may not be a conceptId stream if the owner is an attribute
            Stream<OwnerDatatype> ownerConceptIdStream = ownerPicker.getStream(unityPDF, tx);

            Optional<OwnerDatatype> ownerConceptIdOptional = ownerConceptIdStream.findFirst();

            if (ownerConceptIdOptional.isPresent()) {
                Stream<ValueDatatype> valueStream = valuePicker.getStream(unityPDF, tx);

                OwnerDatatype ownerId = ownerConceptIdOptional.get();
                ValueDatatype value = valueStream.findFirst().get();

                Var o = Graql.var().asUserDefined();  // Owner
                Var a = Graql.var().asUserDefined();  // Attribute

                if (ownerId.getClass() == ConceptId.class) {
                    return (Query) qb.insert(o.has(attributeTypeLabel, a), a.val(value), o.id((ConceptId) ownerId));
                } else {
                    // Both the type and value are required to identify the owner uniquely
                    return (Query) qb.match(o.isa(attributeOwnerStrategy.getTypeLabel()).val(ownerId)).insert(o.has(attributeTypeLabel, a), a.val(value));
                }
            } else {
                return null;
            }
        }).limit(numInstances).filter(Objects::nonNull);
    }
}
