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
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

/**
 * An ontological element used to define different types of rule.
 * Currently supported rules include Constraint Rules and Inference Rules.
 */
class RuleTypeImpl extends TypeImpl<RuleType, Rule> implements RuleType {
    RuleTypeImpl(AbstractGraknGraph graknGraph, Vertex v, Optional<RuleType> type) {
        super(graknGraph, v, type, Optional.empty());
    }

    @Override
    public Rule addRule(Pattern lhs, Pattern rhs) {
        return addInstance(Schema.BaseType.RULE, (vertex, type) ->
                getGraknGraph().getElementFactory().buildRule(vertex, type, lhs, rhs));
    }
}
