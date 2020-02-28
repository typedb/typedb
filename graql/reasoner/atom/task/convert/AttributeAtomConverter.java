package grakn.core.graql.reasoner.atom.task.convert;

import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.ReasonerException;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import java.util.HashSet;
import java.util.Set;

public class AttributeAtomConverter implements AtomConverter<AttributeAtom> {

    @Override
    public AttributeAtom toAttributeAtom(AttributeAtom atom, ReasoningContext ctx){ return atom; }

    @Override
    public RelationAtom toRelationAtom(AttributeAtom atom, ReasoningContext ctx){
        SchemaConcept type = atom.getSchemaConcept();
        if (type == null) throw ReasonerException.illegalAtomConversion(atom, RelationAtom.class);
        Label typeLabel = Schema.ImplicitType.HAS.getLabel(type.label());

        RelationAtom relationAtom = RelationAtom.create(
                Graql.var(atom.getRelationVariable())
                        .rel(Schema.ImplicitType.HAS_OWNER.getLabel(type.label()).getValue(), new Statement(atom.getVarName()))
                        .rel(Schema.ImplicitType.HAS_VALUE.getLabel(type.label()).getValue(), new Statement(atom.getAttributeVariable()))
                        .isa(typeLabel.getValue()),
                atom.getPredicateVariable(),
                ctx.conceptManager().getSchemaConcept(typeLabel).id(),
                atom.getParentQuery(),
                ctx
        );

        Set<Statement> patterns = new HashSet<>(relationAtom.getCombinedPattern().statements());
        atom.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        atom.getMultiPredicate().stream().map(Predicate::getPattern).forEach(patterns::add);
        return ctx.queryFactory().atomic(Graql.and(patterns)).getAtom().toRelationAtom();
    }

    /**
     * NB: this is somewhat ambiguous case -> from {$x has resource $r;} we can extract:
     * - $r isa owner-type;
     * - $x isa attribute-type;
     * We pick the latter as the type information is available.
     *
     * @return corresponding isa atom
     */
    @Override
    public IsaAtom toIsaAtom(AttributeAtom atom, ReasoningContext ctx) {
        IsaAtom isaAtom = IsaAtom.create(atom.getAttributeVariable(), atom.getPredicateVariable(), atom.getTypeId(), false, atom.getParentQuery(), ctx);
        Set<Statement> patterns = new HashSet<>(isaAtom.getCombinedPattern().statements());
        atom.getPredicates().map(Predicate::getPattern).forEach(patterns::add);
        atom.getMultiPredicate().stream().map(Predicate::getPattern).forEach(patterns::add);
        return ctx.queryFactory().atomic(Graql.and(patterns)).getAtom().toIsaAtom();
    }

}
