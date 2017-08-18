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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;

import javax.annotation.Nullable;
import java.util.Objects;

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
public class RuleTypeImpl extends TypeImpl<RuleType, Rule> implements RuleType {
    RuleTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    RuleTypeImpl(VertexElement vertexElement, RuleType type) {
        super(vertexElement, type);
    }

    @Override
    public Rule putRule(Pattern when, Pattern then) {
        Objects.requireNonNull(when);
        Objects.requireNonNull(then);

        return putInstance(Schema.BaseType.RULE,
                () -> getRule(when, then), (vertex, type) -> vertex().graph().factory().buildRule(vertex, type, when, then));
    }

    @Nullable
    private Rule getRule(Pattern when, Pattern then) {
        String index = RuleImpl.generateRuleIndex(this, when, then);
        return vertex().graph().getConcept(Schema.VertexProperty.INDEX, index);
    }
}
