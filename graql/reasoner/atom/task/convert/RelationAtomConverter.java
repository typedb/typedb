package grakn.core.graql.reasoner.atom.task.convert;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.ReasonerException;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Set;

import static graql.lang.Graql.var;

public class RelationAtomConverter implements AtomConverter<RelationAtom> {

    @Override
    public RelationAtom toRelationAtom(RelationAtom atom, ReasoningContext ctx) {
        return atom;
    }

    @Override
    public AttributeAtom toAttributeAtom(RelationAtom atom, ReasoningContext ctx) {
        SchemaConcept type = atom.getSchemaConcept();
        if (type == null || !type.isImplicit()) {
            throw ReasonerException.illegalAtomConversion(atom, AttributeAtom.class);
        }
        ConceptManager conceptManager = ctx.conceptManager();
        Label explicitLabel = Schema.ImplicitType.explicitLabel(type.label());

        Role ownerRole = conceptManager.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(explicitLabel).getValue());
        Role valueRole = conceptManager.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(explicitLabel).getValue());
        Multimap<Role, Variable> roleVarMap = atom.getRoleVarMap();
        Variable relationVariable = atom.getVarName();
        Variable ownerVariable = Iterables.getOnlyElement(roleVarMap.get(ownerRole));
        Variable attributeVariable = Iterables.getOnlyElement(roleVarMap.get(valueRole));

        Statement attributeStatement = relationVariable.isReturned() ?
                var(ownerVariable).has(explicitLabel.getValue(), var(attributeVariable), var(relationVariable)) :
                var(ownerVariable).has(explicitLabel.getValue(), var(attributeVariable));
        AttributeAtom attributeAtom = AttributeAtom.create(
                attributeStatement,
                attributeVariable,
                relationVariable,
                atom.getPredicateVariable(),
                conceptManager.getSchemaConcept(explicitLabel).id(),
                new HashSet<>(),
                atom.getParentQuery(),
                ctx
        );

        Set<Statement> patterns = new HashSet<>(attributeAtom.getCombinedPattern().statements());
        atom.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        return ctx.queryFactory().atomic(Graql.and(patterns)).getAtom().toAttributeAtom();
    }


    @Override
    public IsaAtom toIsaAtom(RelationAtom atom, ReasoningContext ctx) {
        IsaAtom isaAtom = IsaAtom.create(atom.getVarName(), atom.getPredicateVariable(), atom.getTypeId(), false, atom.getParentQuery(), ctx);
        Set<Statement> patterns = new HashSet<>(isaAtom.getCombinedPattern().statements());
        atom.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        return ctx.queryFactory().atomic(Graql.and(patterns)).getAtom().toIsaAtom();
    }
}
