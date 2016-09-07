/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.exception.InvalidConceptValueException;
import io.mindmaps.core.model.Type;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;

/**
 * A rule represents an instance of a Rule Type which is used to make inferences over the data instances.
 */
class RuleImpl extends InstanceImpl<Rule, RuleType> implements Rule {
    RuleImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph, String lhs, String rhs) {
        super(v, mindmapsGraph);
        setRule(DataType.ConceptProperty.RULE_LHS, lhs);
        setRule(DataType.ConceptProperty.RULE_RHS, rhs);
    }

    /**
     *
     * @param rule The rule to mutate
     * @param value The value of either the lhs or rhs set
     */
    private void setRule(DataType.ConceptProperty rule, String value){
        if(value == null){
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(rule.name()));
        }

        if(getProperty(rule) != null){
            String foundRuleValue = (String) getProperty(rule);
            if(!foundRuleValue.equals(value)){
                throw new InvalidConceptValueException(ErrorMessage.IMMUTABLE_VALUE.getMessage(foundRuleValue, this, value, rule.name()));
            }
        } else {
            setProperty(rule, value);
        }
    }

    //TODO: Fill out details on this method
    /**
     *
     * @param expectation
     * @return The Rule itself
     */
    @Override
    public Rule setExpectation(boolean expectation) {
        setProperty(DataType.ConceptProperty.IS_EXPECTED, expectation);
        return getThis();
    }

    //TODO: Fill out details on this method
    /**
     *
     * @param materialise
     * @return The Rule itself
     */
    @Override
    public Rule setMaterialise(boolean materialise) {
        setProperty(DataType.ConceptProperty.IS_MATERIALISED, materialise);
        return getThis();
    }

    /**
     *
     * @return A string representing the left hand side GraQL query.
     */
    @Override
    public String getLHS() {
        Object object = getProperty(DataType.ConceptProperty.RULE_LHS);
        if(object == null)
            return null;
        return (String) object;
    }

    /**
     *
     * @return A string representing the right hand side GraQL query.
     */
    @Override
    public String getRHS() {
        Object object = getProperty(DataType.ConceptProperty.RULE_RHS);
        if(object == null)
            return null;
        return (String) object;
    }


    //TODO: Fill out details on this method
    /**
     *
     * @return
     */
    @Override
    public boolean getExpectation() {
        Object object = getProperty(DataType.ConceptProperty.IS_EXPECTED);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    //TODO: Fill out details on this method
    /**
     *
     * @return
     */
    @Override
    public boolean isMaterialise() {
        Object object = getProperty(DataType.ConceptProperty.IS_MATERIALISED);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    /**
     *
     * @param type The concept type which this rules applies to.
     * @return The Rule itself
     */
    @Override
    public Rule addHypothesis(Type type) {
        putEdge(getMindmapsGraph().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.HYPOTHESIS);
        return getThis();
    }

    /**
     *
     * @param type The concept type which is the conclusion of this Rule.
     * @return The Rule itself
     */
    @Override
    public Rule addConclusion(Type type) {
        putEdge(getMindmapsGraph().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.CONCLUSION);
        return getThis();
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the rule
     */
    @Override
    public Collection<Type> getHypothesisTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HYPOTHESIS).forEach(concept -> types.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(concept)));
        return types;
    }

    /**
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the rule
     */
    @Override
    public Collection<Type> getConclusionTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.CONCLUSION).forEach(concept -> types.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(concept)));
        return types;
    }
}
