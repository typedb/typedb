/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.atom.task.convert;

import com.google.common.collect.Sets;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
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
                typeLabel,
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
        Set<Atomic> atoms = Sets.newHashSet(atom.attributeIsa());
        atom.getPredicates().forEach(atoms::add);
        atoms.addAll(atom.getMultiPredicate());
        return ctx.queryFactory().atomic(atoms).getAtom().toIsaAtom();
    }
}
