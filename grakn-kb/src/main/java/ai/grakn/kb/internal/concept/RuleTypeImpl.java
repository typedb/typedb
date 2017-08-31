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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

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

    RuleTypeImpl(VertexElement vertexElement, RuleType type, Pattern when, Pattern then) {
        super(vertexElement, type);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_WHEN, when, getWhen(), Pattern::toString);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_THEN, then, getThen(), Pattern::toString);
        vertex().propertyUnique(Schema.VertexProperty.INDEX, generateRuleIndex(sup(), when, then));
    }

    @Override
    public Pattern getWhen() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_WHEN));
    }

    @Override
    public Pattern getThen() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_THEN));
    }

    @Override
    public Stream<Type> getHypothesisTypes() {
        return neighbours(Direction.OUT, Schema.EdgeLabel.HYPOTHESIS);
    }

    @Override
    public Stream<Type> getConclusionTypes() {
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
            return vertex().tx().graql().parsePattern(value);
        }
    }

    /**
     * Generate the internal hash in order to perform a faster lookups and ensure rules are unique
     */
    static String generateRuleIndex(RuleType type, Pattern when, Pattern then){
        return "RuleType_" + type.getLabel().getValue() + "_LHS:" + when.hashCode() + "_RHS:" + then.hashCode();
    }

    public static <X extends Type, Y extends Thing> RuleTypeImpl from(RuleType type){
        //noinspection unchecked
        return (RuleTypeImpl) type;
    }
}
