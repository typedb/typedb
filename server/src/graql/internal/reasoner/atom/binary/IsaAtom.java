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

package grakn.core.graql.internal.reasoner.atom.binary;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.server.kb.concept.ConceptUtils;
import grakn.core.server.kb.concept.EntityTypeImpl;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;

/**
 * TypeAtom corresponding to graql a {@link IsaProperty} property.
 */
@AutoValue
public abstract class IsaAtom extends IsaAtomBase {

    @Override public abstract Variable getPredicateVariable();
    @Override public abstract Statement getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static IsaAtom create(Variable var, Variable predicateVar, Statement pattern, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_IsaAtom(var, predicateId, predicateVar, pattern, parent);
    }

    public static IsaAtom create(Variable var, Variable predicateVar, @Nullable ConceptId predicateId, boolean isDirect, ReasonerQuery parent) {
        Statement pattern = isDirect ?
                new Statement(var).isaX(new Statement(predicateVar)) :
                new Statement(var).isa(new Statement(predicateVar));

        return new AutoValue_IsaAtom(var, predicateId, predicateVar, pattern, parent);
    }

    public static IsaAtom create(Variable var, Variable predicateVar, SchemaConcept type, boolean isDirect, ReasonerQuery parent) {
        Statement pattern = isDirect ?
                new Statement(var).isaX(new Statement(predicateVar)) :
                new Statement(var).isa(new Statement(predicateVar));

        return new AutoValue_IsaAtom(var, type.id(), predicateVar, pattern, parent);
    }
    private static IsaAtom create(IsaAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getPattern(), a.getTypeId(), parent);
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
                && ((this.getTypeId() == null) ? (that.getTypeId() == null) : this.getTypeId().equals(that.getTypeId()));
    }

    @Memoized
    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + getVarName().hashCode();
        hashCode = hashCode * 37 + (getTypeId() != null ? getTypeId().hashCode() : 0);
        return hashCode;
    }

    @Override
    public String toString(){
        String typeString = (getSchemaConcept() != null? getSchemaConcept().label() : "") + "(" + getVarName() + ")";
        return typeString +
                (getPredicateVariable().isUserDefinedName()? "(" + getPredicateVariable() + ")" : "") +
                (isDirect()? "!" : "") +
                getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected Pattern createCombinedPattern(){
        if (getPredicateVariable().isUserDefinedName()) return super.createCombinedPattern();
        return getSchemaConcept() == null?
                new Statement(getVarName()).isa(new Statement(getPredicateVariable())) :
                isDirect()?
                        new Statement(getVarName()).isaX(getSchemaConcept().label().getValue()) :
                        new Statement(getVarName()).isa(getSchemaConcept().label().getValue()) ;
    }

    @Override
    public IsaAtom addType(SchemaConcept type) {
        if (getTypeId() != null) return this;
        return create(getVarName(), getPredicateVariable(), type.id(), this.isDirect(), this.getParentQuery());
    }

    private IsaAtom inferEntityType(ConceptMap sub){
        if (getTypePredicate() != null) return this;
        if (sub.containsVar(getPredicateVariable())) return addType(sub.get(getPredicateVariable()).asType());
        return this;
    }

    private ImmutableList<Type> inferPossibleTypes(ConceptMap sub){
        if (getSchemaConcept() != null) return ImmutableList.of(getSchemaConcept().asType());
        if (sub.containsVar(getPredicateVariable())) return ImmutableList.of(sub.get(getPredicateVariable()).asType());

        //determine compatible types from played roles
        Set<Type> typesFromRoles = getParentQuery().getAtoms(RelationshipAtom.class)
                .filter(r -> r.getVarNames().contains(getVarName()))
                .flatMap(r -> r.getRoleVarMap().entries().stream()
                        .filter(e -> e.getValue().equals(getVarName()))
                        .map(Map.Entry::getKey))
                .map(role -> role.players().collect(Collectors.toSet()))
                .reduce(Sets::intersection)
                .orElse(Sets.newHashSet());

        Set<Type> typesFromTypes = getParentQuery().getAtoms(IsaAtom.class)
                .filter(at -> at.getVarNames().contains(getVarName()))
                .filter(at -> at != this)
                .map(Binary::getSchemaConcept)
                .filter(Objects::nonNull)
                .filter(Concept::isType)
                .map(Concept::asType)
                .collect(Collectors.toSet());

        Set<Type> types = typesFromTypes.isEmpty()?
                typesFromRoles :
                typesFromRoles.isEmpty()? typesFromTypes: Sets.intersection(typesFromRoles, typesFromTypes);

        return !types.isEmpty()?
                ImmutableList.copyOf(ConceptUtils.top(types)) :
                tx().getMetaConcept().subs().collect(toImmutableList());
    }

    @Override
    public IsaAtom inferTypes(ConceptMap sub) {
        return this
                .inferEntityType(sub);
    }

    @Override
    public ImmutableList<Type> getPossibleTypes() { return inferPossibleTypes(new ConceptMap()); }

    @Override
    public List<Atom> atomOptions(ConceptMap sub) {
        return this.inferPossibleTypes(sub).stream()
                .map(this::addType)
                .sorted(Comparator.comparing(Atom::isRuleResolvable))
                .collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptMap> materialise(){
        EntityType entityType = getSchemaConcept().asEntityType();
        return Stream.of(
                getParentQuery().getSubstitution()
                        .merge(new ConceptMap(ImmutableMap.of(getVarName(), EntityTypeImpl.from(entityType).addEntityInferred())))
        );
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getVarName(), getPredicateVariable().asUserDefined(), getTypeId(), this.isDirect(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return this.rewriteWithTypeVariable(parentAtom);
    }
}
