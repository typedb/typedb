/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * <p>
 *     An ontological element used to model and categorise different types of {@link Rule}.
 * </p>
 *
 * <p>
 *     An ontological element used to define different types of {@link Rule}.
 *     Currently supported rules include {@link GraknAdmin#getMetaRuleInference()} and {@link GraknAdmin#getMetaRuleConstraint()}
 * </p>
 *
 * @author fppt
 */
class RuleTypeImpl extends TypeImpl<RuleType, Rule> implements RuleType {
    RuleTypeImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    RuleTypeImpl(AbstractGraknGraph graknGraph, Vertex v, RuleType type) {
        super(graknGraph, v, type);
    }

    private RuleTypeImpl(RuleTypeImpl rule){
        super(rule);
    }

    @Override
    public RuleType copy(){
        return new RuleTypeImpl(this);
    }

    @Override
    public Rule putRule(Pattern lhs, Pattern rhs) {
        if(lhs == null) {
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(Schema.ConceptProperty.RULE_LHS.name()));
        }

        if(rhs == null) {
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(Schema.ConceptProperty.RULE_RHS.name()));
        }

        return putInstance(Schema.BaseType.RULE,
                () -> getRule(lhs, rhs), (vertex, type) ->
                getGraknGraph().getElementFactory().buildRule(vertex, type, lhs, rhs));
    }

    private Rule getRule(Pattern lhs, Pattern rhs) {
        String index = RuleImpl.generateRuleIndex(this, lhs, rhs);
        return getGraknGraph().getConcept(Schema.ConceptProperty.INDEX, index);
    }
}
