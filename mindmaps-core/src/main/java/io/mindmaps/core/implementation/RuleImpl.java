package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Type;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;

class RuleImpl extends InstanceImpl<Rule, RuleType, String> implements Rule {
    RuleImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public Rule setLHS(String lhs) {
        setProperty(DataType.ConceptProperty.RULE_LHS, lhs);
        return getThis();
    }

    @Override
    public Rule setRHS(String rhs) {
        setProperty(DataType.ConceptProperty.RULE_RHS, rhs);
        return getThis();
    }

    @Override
    public Rule setExpectation(boolean expectation) {
        setProperty(DataType.ConceptProperty.IS_EXPECTED, expectation);
        return getThis();
    }

    @Override
    public Rule setMaterialise(boolean materialise) {
        setProperty(DataType.ConceptProperty.IS_MATERIALISED, materialise);
        return getThis();
    }

    @Override
    public String getLHS() {
        Object object = getProperty(DataType.ConceptProperty.RULE_LHS);
        if(object == null)
            return null;
        return (String) object;
    }

    @Override
    public String getRHS() {
        Object object = getProperty(DataType.ConceptProperty.RULE_RHS);
        if(object == null)
            return null;
        return (String) object;
    }

    @Override
    public boolean getExpectation() {
        Object object = getProperty(DataType.ConceptProperty.IS_EXPECTED);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    @Override
    public boolean isMaterialise() {
        Object object = getProperty(DataType.ConceptProperty.IS_MATERIALISED);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    @Override
    public Rule addHypothesis(Type type) {
        putEdge(getMindmapsGraph().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.HYPOTHESIS);
        return getThis();
    }

    @Override
    public Rule addConclusion(Type type) {
        putEdge(getMindmapsGraph().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.CONCLUSION);
        return getThis();
    }

    @Override
    public Collection<Type> getHypothesisTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HYPOTHESIS).forEach(concept -> {
            types.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(concept));
        });
        return types;
    }

    @Override
    public Collection<Type> getConclusionTypes() {
        Collection<Type> types = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.CONCLUSION).forEach(concept -> {
            types.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(concept));
        });
        return types;
    }
}
