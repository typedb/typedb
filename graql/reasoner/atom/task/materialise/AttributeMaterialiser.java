/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.reasoner.atom.task.materialise;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.core.AttributeValueConverter;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.stream.Stream;

public class AttributeMaterialiser implements AtomMaterialiser<AttributeAtom> {

    @Override
    public Stream<ConceptMap> materialise(AttributeAtom atom, ReasoningContext ctx) {
        ConceptMap substitution = atom.getParentQuery().getSubstitution();
        AttributeType<Object> attributeType = atom.getSchemaConcept().asAttributeType();

        Variable varName = atom.getVarName();
        Concept owner = substitution.get(varName);
        Variable resourceVariable = atom.getAttributeVariable();

        //if the attribute already exists, only attach a new link to the owner, otherwise create a new attribute
        Attribute attribute = null;
        if (atom.isValueEquality()) {
            ValuePredicate vp = Iterables.getOnlyElement(atom.getMultiPredicate());
            Object value = vp.getPredicate().value();
            // read attr if exists
            Attribute existingAttribute = attributeType.attribute(value);
            // write attribute if it doesn't
            attribute = existingAttribute == null ? attributeType.putAttributeInferred(value) : existingAttribute;
        } else {
            Attribute existingAttribute = substitution.containsVar(resourceVariable) ? substitution.get(resourceVariable).asAttribute() : null;
            //even if the attribute exists but is of different type (supertype for instance) we create a new one
            //to make sure the attribute index will be different
            if (existingAttribute != null) {
                Object value = existingAttribute.value();
                attribute = existingAttribute;
                if (!existingAttribute.type().equals(attributeType)) {
                    existingAttribute = attributeType.attribute(value);
                    attribute = existingAttribute == null ? attributeType.putAttributeInferred(value) : existingAttribute;
                }
            }
        }

        if (attribute != null) {
            ConceptMap answer = new ConceptMap(ImmutableMap.of(
                    varName, substitution.get(varName),
                    resourceVariable, attribute)
            );

            putAttributeOwmership(atom, answer, owner, attribute, ctx);
            return Stream.of(answer);
        }
        return Stream.empty();
    }

    private ConceptMap findAnswer(Atom atom, ConceptMap sub, ReasoningContext ctx) {
        //NB: we are only interested in this atom and its subs, not any other constraints
        ReasonerAtomicQuery query = ctx.queryFactory().atomic(Collections.singleton(atom)).withSubstitution(sub);
        MultilevelSemanticCache queryCacheImpl = CacheCasting.queryCacheCast(ctx.queryCache());
        ConceptMap answer = queryCacheImpl.getAnswerStream(query).findFirst().orElse(null);

        if (answer == null) queryCacheImpl.ackDBCompleteness(query);
        else queryCacheImpl.record(query.withSubstitution(answer), answer);
        return answer;
    }


    /**
     * @param sub       partial substitution
     * @param owner     attribute owner
     * @param attribute attribute concept
     */
    private void putAttributeOwmership(AttributeAtom atom, ConceptMap sub, Concept owner, Attribute attribute, ReasoningContext ctx) {
        ConceptMap answer = findAnswer(atom, sub, ctx);
        if (answer == null) owner.asThing().attributeInferred(attribute);
    }
}
