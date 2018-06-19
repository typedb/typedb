/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.DirectIsaProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;

import ai.grakn.graql.internal.reasoner.utils.Pair;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.kb.internal.concept.EntityTypeImpl;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static ai.grakn.util.CommonUtil.toImmutableList;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link ai.grakn.graql.internal.pattern.property.IsaProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class IsaAtom extends IsaAtomBase {

    @Override public abstract Var getPredicateVariable();
    @Override public abstract VarPattern getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static IsaAtom create(VarPattern pattern, Var predicateVar, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_IsaAtom(pattern.admin().var(), predicateId, predicateVar, pattern, parent);
    }
    private static IsaAtom create(Var var, Var predicateVar, @Nullable ConceptId predicateId, ReasonerQuery parent) {
        return create(var.isa(predicateVar).admin(), predicateVar, predicateId, parent);
    }
    public static IsaAtom create(Var var, Var predicateVar, SchemaConcept type, ReasonerQuery parent) {
        return create(var, predicateVar, type.getId(), parent);
    }
    private static IsaAtom create(IsaAtom a, ReasonerQuery parent) {
        IsaAtom atom = create(a.getPattern(), a.getPredicateVariable(), a.getTypeId(), parent);
        atom.applicableRules = a.applicableRules;
        return atom;
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return isDirect()? DirectIsaProperty.class : IsaProperty.class;}

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
        String typeString = (getSchemaConcept() != null? getSchemaConcept().getLabel() : "") + "(" + getVarName() + ")";
        return typeString +
                (isDirect()? "!" : "") +
                getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected Pattern createCombinedPattern(){
        if (getPredicateVariable().isUserDefinedName()) return super.createCombinedPattern();
        return getSchemaConcept() == null?
                getVarName().isa(getPredicateVariable()) :
                isDirect()?
                        getVarName().directIsa(getSchemaConcept().getLabel().getValue()) :
                        getVarName().isa(getSchemaConcept().getLabel().getValue()) ;
    }

    @Override
    public IsaAtom addType(SchemaConcept type) {
        if (getTypeId() != null) return this;
        Pair<VarPattern, IdPredicate> typedPair = getTypedPair(type);
        return create(typedPair.getKey(), typedPair.getValue().getVarName(), typedPair.getValue().getPredicate(), this.getParentQuery());
    }

    private IsaAtom inferEntityType(Answer sub){
        if (getTypePredicate() != null) return this;
        if (sub.containsVar(getPredicateVariable())) return addType(sub.get(getPredicateVariable()).asType());
        return this;
    }

    private ImmutableList<Type> inferPossibleTypes(Answer sub){
        if (getSchemaConcept() != null) return ImmutableList.of(this.getSchemaConcept().asType());
        if (sub.containsVar(getPredicateVariable())) return ImmutableList.of(sub.get(getPredicateVariable()).asType());

        //determine compatible types from played roles
        Set<Type> types = getParentQuery().getAtoms(RelationshipAtom.class)
                .filter(r -> r.getVarNames().contains(getVarName()))
                .flatMap(r -> r.getRoleVarMap().entries().stream()
                        .filter(e -> e.getValue().equals(getVarName()))
                        .map(Map.Entry::getKey))
                .map(role -> role.playedByTypes().collect(Collectors.toSet()))
                .reduce(Sets::intersection)
                .orElse(Sets.newHashSet());

        return !types.isEmpty()?
                ImmutableList.copyOf(ReasonerUtils.top(types)) :
                tx().admin().getMetaConcept().subs().collect(toImmutableList());
    }

    @Override
    public IsaAtom inferTypes(Answer sub) {
        return this
                .inferEntityType(sub);
    }

    @Override
    public ImmutableList<Type> getPossibleTypes() { return inferPossibleTypes(new QueryAnswer()); }

    @Override
    public List<Atom> atomOptions(Answer sub) {
        return this.inferPossibleTypes(sub).stream()
                .map(this::addType)
                .sorted(Comparator.comparing(Atom::isRuleResolvable))
                .collect(Collectors.toList());
    }

    @Override
    public Stream<Answer> materialise(){
        EntityType entityType = getSchemaConcept().asEntityType();
        return Stream.of(
                getParentQuery().getSubstitution()
                        .merge(new QueryAnswer(ImmutableMap.of(getVarName(), EntityTypeImpl.from(entityType).addEntityInferred())))
        );
    }

    @Override
    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> create(v, getPredicateVariable(), getTypeId(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getPattern(), getPredicateVariable().asUserDefined(), getTypeId(), getParentQuery());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return parentAtom.getPredicateVariable().isUserDefinedName()?
                rewriteWithTypeVariable() :
                this;
    }
}
