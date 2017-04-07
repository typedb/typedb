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
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;

/**
 * <p>
 *     A rule which defines how implicit knowledge can extracted.
 * </p>
 *
 * <p>
 *     It can behave like any other {@link ai.grakn.concept.Instance} but primarily serves as a way of extracting
 *     implicit data from the graph. By defining the LHS (if statment) and RHS (then conclusion) it is possible to
 *     automatically materialise new concepts based on these rules.
 * </p>
 *
 * @author fppt
 *
 */
class RuleImpl extends InstanceImpl<Rule, RuleType> implements Rule {
    RuleImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    RuleImpl(AbstractGraknGraph graknGraph, Vertex v, RuleType type, Pattern lhs, Pattern rhs) {
        super(graknGraph, v, type);
        setImmutableProperty(Schema.ConceptProperty.RULE_LHS, lhs, getLHS(), Pattern::toString);
        setImmutableProperty(Schema.ConceptProperty.RULE_RHS, rhs, getRHS(), Pattern::toString);
        setUniqueProperty(Schema.ConceptProperty.INDEX, generateRuleIndex(type, lhs, rhs));
    }

    /**
     *
     * @return A string representing the left hand side GraQL query.
     */
    @Override
    public Pattern getLHS() {
        return parsePattern(getProperty(Schema.ConceptProperty.RULE_LHS));
    }

    /**
     *
     * @return A string representing the right hand side GraQL query.
     */
    @Override
    public Pattern getRHS() {
        return parsePattern(getProperty(Schema.ConceptProperty.RULE_RHS));
    }

    private Pattern parsePattern(String value){
        if(value == null) {
            return null;
        } else {
            return getGraknGraph().graql().parsePattern(value);
        }
    }

    /**
     *
     * @param type The concept type which this rules applies to.
     * @return The Rule itself
     */
    Rule addHypothesis(Type type) {
        putEdge(type, Schema.EdgeLabel.HYPOTHESIS);
        return getThis();
    }

    /**
     *
     * @param type The concept type which is the conclusion of this Rule.
     * @return The Rule itself
     */
    Rule addConclusion(Type type) {
        putEdge(type, Schema.EdgeLabel.CONCLUSION);
        return getThis();
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the rule
     */
    @Override
    public Collection<Type> getHypothesisTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> types.add(concept.asType()));
        return types;
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the rule
     */
    @Override
    public Collection<Type> getConclusionTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.CONCLUSION).forEach(concept -> types.add(concept.asType()));
        return types;
    }

    /**
     * Generate the internal hash in order to perform a faster lookups and ensure rules are unique
     */
    public static String generateRuleIndex(RuleType type, Pattern lhs, Pattern rhs){
        return "RuleType_" + type.getLabel().getValue() + "_LHS:" + lhs.hashCode() + "_RHS:" + rhs.hashCode();
    }
}
