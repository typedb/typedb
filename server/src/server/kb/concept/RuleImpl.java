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

import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.Pattern;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.graql.internal.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element used to model and categorise different types of {@link Rule}.
 * </p>
 *
 * <p>
 *     An ontological element used to define different types of {@link Rule}.
 * </p>
 *
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

    public static RuleImpl get(VertexElement vertexElement){
        return new RuleImpl(vertexElement);
    }

    public static RuleImpl create(VertexElement vertexElement, Rule type, Pattern when, Pattern then) {
        RuleImpl rule = new RuleImpl(vertexElement, type, when, then);
        vertexElement.tx().txCache().trackForValidation(rule);
        return rule;
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
     *
     * @param type The {@link Type} which this {@link Rule} applies to.
     */
    public void addHypothesis(Type type) {
        putEdge(ConceptVertex.from(type), Schema.EdgeLabel.HYPOTHESIS);
    }

    /**
     *
     * @param type The {@link Type} which is the conclusion of this {@link Rule}.
     */
    public void addConclusion(Type type) {
        putEdge(ConceptVertex.from(type), Schema.EdgeLabel.CONCLUSION);
    }

    private Pattern parsePattern(String value){
        if(value == null) {
            return null;
        } else {
            return vertex().tx().graql().parser().parsePattern(value);
        }
    }

    public static <X extends Type, Y extends Thing> RuleImpl from(Rule type){
        //noinspection unchecked
        return (RuleImpl) type;
    }
}
