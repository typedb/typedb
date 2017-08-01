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

import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.HashSet;

/**
 * <p>
 *     A rule which defines how implicit knowledge can extracted.
 * </p>
 *
 * <p>
 *     It can behave like any other {@link Thing} but primarily serves as a way of extracting
 *     implicit data from the graph. By defining the LHS (if statment) and RHS (then conclusion) it is possible to
 *     automatically materialise new concepts based on these rules.
 * </p>
 *
 * @author fppt
 *
 */
public class RuleImpl extends ThingImpl<Rule, RuleType> implements Rule {
    RuleImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    RuleImpl(VertexElement vertexElement, RuleType type, Pattern when, Pattern then) {
        super(vertexElement, type);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_WHEN, when, getWhen(), Pattern::toString);
        vertex().propertyImmutable(Schema.VertexProperty.RULE_THEN, then, getThen(), Pattern::toString);
        vertex().propertyUnique(Schema.VertexProperty.INDEX, generateRuleIndex(type(), when, then));
    }

    /**
     *
     * @return A string representing the left hand side GraQL query.
     */
    @Override
    public Pattern getWhen() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_WHEN));
    }

    /**
     *
     * @return A string representing the right hand side GraQL query.
     */
    @Override
    public Pattern getThen() {
        return parsePattern(vertex().property(Schema.VertexProperty.RULE_THEN));
    }

    private Pattern parsePattern(String value){
        if(value == null) {
            return null;
        } else {
            return vertex().graph().graql().parsePattern(value);
        }
    }

    /**
     *
     * @param ontologyConcept The {@link OntologyConcept} which this {@link Rule} applies to.
     * @return The {@link Rule} itself
     */
    public void addHypothesis(OntologyConcept ontologyConcept) {
        putEdge(ConceptVertex.from(ontologyConcept), Schema.EdgeLabel.HYPOTHESIS);
    }

    /**
     *
     * @param ontologyConcept The {@link OntologyConcept} which is the conclusion of this {@link Rule}.
     * @return The {@link Rule} itself
     */
    public void addConclusion(OntologyConcept ontologyConcept) {
        putEdge(ConceptVertex.from(ontologyConcept), Schema.EdgeLabel.CONCLUSION);
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the rule
     */
    @Override
    public Collection<Type> getHypothesisTypes() {
        Collection<Type> types = new HashSet<>();
        neighbours(Direction.OUT, Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> types.add(concept.asType()));
        return types;
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the rule
     */
    @Override
    public Collection<Type> getConclusionTypes() {
        Collection<Type> types = new HashSet<>();
        neighbours(Direction.OUT, Schema.EdgeLabel.CONCLUSION).forEach(concept -> types.add(concept.asType()));
        return types;
    }

    /**
     * Generate the internal hash in order to perform a faster lookups and ensure rules are unique
     */
    static String generateRuleIndex(RuleType type, Pattern when, Pattern then){
        return "RuleType_" + type.getLabel().getValue() + "_LHS:" + when.hashCode() + "_RHS:" + then.hashCode();
    }

    public static RuleImpl from(Rule rule){
        return (RuleImpl) rule;
    }
}
