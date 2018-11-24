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

import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.query.pattern.VarPattern;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.predicate.Predicate;

import grakn.core.graql.internal.reasoner.utils.ReasonerUtils;
import grakn.core.server.kb.concept.EntityTypeImpl;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static grakn.core.common.util.CommonUtil.toImmutableList;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link IsaProperty} property.
 * </p>
 *
 *
 */
@AutoValue
public abstract class IsaAtom extends IsaAtomBase {

    @Override public abstract Var getPredicateVariable();
    @Override public abstract VarPattern getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static IsaAtom create(Var var, Var predicateVar, VarPattern pattern, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_IsaAtom(var, predicateId, predicateVar, pattern, parent);
    }

    public static IsaAtom create(Var var, Var predicateVar, @Nullable ConceptId predicateId, boolean isDirect, ReasonerQuery parent) {
        VarPattern pattern = isDirect ? var.isaExplicit(predicateVar).admin() : var.isa(predicateVar).admin();
        return new AutoValue_IsaAtom(var, predicateId, predicateVar, pattern, parent);
    }

    public static IsaAtom create(Var var, Var predicateVar, SchemaConcept type, boolean isDirect, ReasonerQuery parent) {
        VarPattern pattern = isDirect ? var.isaExplicit(predicateVar).admin() : var.isa(predicateVar).admin();
        return new AutoValue_IsaAtom(var, type.id(), predicateVar, pattern, parent);
    }
    private static IsaAtom create(IsaAtom a, ReasonerQuery parent) {
        IsaAtom atom = create(a.getVarName(), a.getPredicateVariable(), a.getPattern(), a.getTypeId(), parent);
        atom.applicableRules = a.applicableRules;
        return atom;
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public IsaAtom toIsaAtom(){ return this; }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return isDirect()? IsaExplicitProperty.class : IsaProperty.class;}

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
                getVarName().isa(getPredicateVariable()) :
                isDirect()?
                        getVarName().isaExplicit(getSchemaConcept().label().getValue()) :
                        getVarName().isa(getSchemaConcept().label().getValue()) ;
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

    private ImmutableList<SchemaConcept> inferPossibleTypes(ConceptMap sub){
        if (getSchemaConcept() != null) return ImmutableList.of(this.getSchemaConcept());
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
                ImmutableList.copyOf(ReasonerUtils.top(types)) :
                tx().getMetaConcept().subs().collect(toImmutableList());
    }

    @Override
    public IsaAtom inferTypes(ConceptMap sub) {
        return this
                .inferEntityType(sub);
    }

    @Override
    public ImmutableList<SchemaConcept> getPossibleTypes() { return inferPossibleTypes(new ConceptMap()); }

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
