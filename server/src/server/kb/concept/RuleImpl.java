/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.concept;

import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.Type;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.VertexElement;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * An ontological element used to model and categorise different types of Rule.
 * An ontological element used to define different types of Rule.
 */
public class RuleImpl extends SchemaConceptImpl<Rule> implements Rule {
    private RuleImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RuleImpl(VertexElement vertexElement, Rule type, Pattern when, Pattern then) {
        super(vertexElement, type);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_WHEN, when, when(), Pattern::toString);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_THEN, then, then(), Pattern::toString);
    }

    public static RuleImpl get(VertexElement vertexElement) {
        return new RuleImpl(vertexElement);
    }

    public static RuleImpl create(VertexElement vertexElement, Rule type, Pattern when, Pattern then) {
        RuleImpl rule = new RuleImpl(vertexElement, type, when, then);
        vertexElement.tx().cache().trackForValidation(rule);
        return rule;
    }

    public static <X extends Type, Y extends Thing> RuleImpl from(Rule type) {
        //noinspection unchecked
        return (RuleImpl) type;
    }

    @Override
    void trackRolePlayers() {
        //TODO: CLean this up
    }

    @Override
    public Pattern when() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_WHEN));
    }

    @Override
    public Pattern then() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_THEN));
    }

    @Override
    public Stream<Type> whenTypes() {
        return neighbours(Direction.OUT, Schema.EdgeLabel.HYPOTHESIS);
    }

    @Override
    public Stream<Type> thenTypes() {
        return neighbours(Direction.OUT, Schema.EdgeLabel.CONCLUSION);
    }

    /**
     * @param type The Type which this Rule applies to.
     */
    public void addHypothesis(Type type) {
        putEdge(ConceptVertex.from(type), Schema.EdgeLabel.HYPOTHESIS);
    }

    /**
     * @param type The Type which is the conclusion of this Rule.
     */
    public void addConclusion(Type type) {
        putEdge(ConceptVertex.from(type), Schema.EdgeLabel.CONCLUSION);
    }

    private Pattern parsePattern(String value) {
        if (value == null) {
            return null;
        } else {
            return Graql.parsePattern(value);
        }
    }
}
