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

package grakn.core.graql.reasoner.atom.binary;

import com.google.common.collect.ImmutableList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.task.infer.IsaTypeReasoner;
import grakn.core.graql.reasoner.atom.task.infer.TypeReasoner;
import grakn.core.graql.reasoner.atom.task.materialise.IsaMaterialiser;
import grakn.core.graql.reasoner.atom.task.validate.IsaAtomValidator;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * TypeAtom corresponding to graql a IsaProperty property.
 */
public class IsaAtom extends TypeAtom {

    private final TypeReasoner<IsaAtom> typeReasoner = new IsaTypeReasoner();

    private int hashCode;
    private boolean hashCodeMemoised;

    private IsaAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label, Variable predicateVariable,
                    ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, label, predicateVariable, ctx);
    }

    public static IsaAtom create(Variable var, Variable predicateVar, @Nullable Label label, boolean isDirect, ReasonerQuery parent,
                                 ReasoningContext ctx) {
        Statement pattern = isDirect?
                new Statement(var).isaX(new Statement(predicateVar)) :
                new Statement(var).isa(new Statement(predicateVar));
        return new IsaAtom(var, pattern, parent, label, predicateVar, ctx);
    }

    private static IsaAtom create(IsaAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeLabel(), a.isDirect(), parent, a.context());
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public IsaAtom toIsaAtom(){ return this; }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {
        return IsaProperty.class;
    }

    //NB: overriding as these require a derived property
    @Override
    public final boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        IsaAtom that = (IsaAtom) obj;
        return this.getVarName().equals(that.getVarName())
                && this.isDirect() == that.isDirect()
                && ((this.getTypeLabel() == null) ? (that.getTypeLabel() == null) : this.getTypeLabel().equals(that.getTypeLabel()));
    }

    @Override
    public int hashCode() {
        if (!hashCodeMemoised) {
            hashCode = Objects.hash(getVarName(), getTypeLabel());
            hashCodeMemoised = true;
        }
        return hashCode;
    }

    @Override
    public String toString(){
        Label typeLabel = getTypeLabel();
        String typeString = (typeLabel != null? typeLabel : "") + "(" + getVarName() + ")";
        return typeString +
                (getPredicateVariable().isReturned()? "(" + getPredicateVariable() + ")" : "") +
                (isDirect()? "!" : "") +
                getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected Pattern createCombinedPattern(){
        if (getPredicateVariable().isReturned()) return super.createCombinedPattern();
        return getTypeLabel() == null?
                new Statement(getVarName()).isa(new Statement(getPredicateVariable())) :
                isDirect()?
                        new Statement(getVarName()).isaX(getTypeLabel().getValue()) :
                        new Statement(getVarName()).isa(getTypeLabel().getValue()) ;
    }

    @Override
    public IsaAtom addType(SchemaConcept type) {
        if (getTypeLabel() != null) return this;
        return create(getVarName(), getPredicateVariable(), type.label(), this.isDirect(), this.getParentQuery(), this.context());
    }

    @Override
    public void checkValid(){
        super.checkValid();
        (new IsaAtomValidator()).checkValid(this, context());
    }

    @Override
    public IsaAtom inferTypes(ConceptMap sub) {
        return typeReasoner.inferTypes(this, sub, context());
    }

    @Override
    public ImmutableList<Type> getPossibleTypes() {
        return typeReasoner.inferPossibleTypes(this, new ConceptMap(), context());
    }

    @Override
    public Stream<ConceptMap> materialise() {
        return new IsaMaterialiser().materialise(this, context());
    }

    @Override
    public List<Atom> atomOptions(ConceptMap sub) {
        return typeReasoner.atomOptions(this, sub, context());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getVarName(), getPredicateVariable().asReturnedVar(), getTypeLabel(), this.isDirect(), getParentQuery(), this.context());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return this.rewriteWithTypeVariable(parentAtom);
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierType unifierType) {
        //in general this <= parent, so no specialisation viable
        if (this.getClass() != parentAtom.getClass()) return UnifierImpl.nonExistent();
        return super.getUnifier(parentAtom, unifierType);
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom parentAtom, UnifierType unifierType) {
        Unifier unifier = this.getUnifier(parentAtom, unifierType);
        return unifier != null ? new MultiUnifierImpl(unifier) : MultiUnifierImpl.nonExistent();
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Variable> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream()
                        .map(v -> IsaAtom.create(v, getPredicateVariable(), getTypeLabel(), this.isDirect(), this.getParentQuery(), this.context()))
                        .collect(Collectors.toSet());
    }
}
