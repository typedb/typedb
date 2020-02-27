/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.inference.IsaTypeReasoner;
import grakn.core.graql.reasoner.atom.inference.TypeReasoner;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * TypeAtom corresponding to graql a IsaProperty property.
 */
public class IsaAtom extends IsaAtomBase {

    private final TypeReasoner<IsaAtom> typeReasoner;

    private int hashCode;
    private boolean hashCodeMemoised;

    private IsaAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, ConceptId typeId,
                    Variable predicateVariable, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, typeId, predicateVariable, ctx);
        this.typeReasoner = new IsaTypeReasoner(ctx.conceptManager());
    }

    public static IsaAtom create(Variable var, Variable predicateVar, Statement pattern, @Nullable ConceptId predicateId, ReasonerQuery parent, ReasoningContext ctx) {
        return new IsaAtom(var.asReturnedVar(), pattern, parent, predicateId, predicateVar, ctx);
    }

    public static IsaAtom create(Variable var, Variable predicateVar, @Nullable ConceptId predicateId, boolean isDirect, ReasonerQuery parent, ReasoningContext ctx) {
        Statement pattern = isDirect ?
                new Statement(var).isaX(new Statement(predicateVar)) :
                new Statement(var).isa(new Statement(predicateVar));

        return new IsaAtom(var, pattern, parent, predicateId, predicateVar, ctx);
    }

    public static IsaAtom create( Variable var, Variable predicateVar, SchemaConcept type, boolean isDirect, ReasonerQuery parent, ReasoningContext ctx) {
        Statement pattern = isDirect ?
                new Statement(var).isaX(new Statement(predicateVar)) :
                new Statement(var).isa(new Statement(predicateVar));
        return new IsaAtom(var, pattern, parent, type.id(), predicateVar, ctx);
    }

    private static IsaAtom create(IsaAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getPattern(), a.getTypeId(), parent, a.context());
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

    @Override
    public void checkValid(){
        super.checkValid();
        SchemaConcept type = getSchemaConcept();
        if (type != null && !type.isType()) {
            throw GraqlSemanticException.cannotGetInstancesOfNonType(type.label());
        }
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

    @Override
    public int hashCode() {
        if (!hashCodeMemoised) {
            hashCode = Objects.hash(getVarName(), getTypeId());
            hashCodeMemoised = true;
        }
        return hashCode;
    }

    @Override
    public String toString(){
        String typeString = (getSchemaConcept() != null? getSchemaConcept().label() : "") + "(" + getVarName() + ")";
        return typeString +
                (getPredicateVariable().isReturned()? "(" + getPredicateVariable() + ")" : "") +
                (isDirect()? "!" : "") +
                getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected Pattern createCombinedPattern(){
        if (getPredicateVariable().isReturned()) return super.createCombinedPattern();
        return getSchemaConcept() == null?
                new Statement(getVarName()).isa(new Statement(getPredicateVariable())) :
                isDirect()?
                        new Statement(getVarName()).isaX(getSchemaConcept().label().getValue()) :
                        new Statement(getVarName()).isa(getSchemaConcept().label().getValue()) ;
    }

    @Override
    public IsaAtom addType(SchemaConcept type) {
        if (getTypeId() != null) return this;
        return create(getVarName(), getPredicateVariable(), type.id(), this.isDirect(), this.getParentQuery(), this.context());
    }

    @Override
    public IsaAtom inferTypes(ConceptMap sub) {
        return typeReasoner.inferTypes(this, sub);
    }

    @Override
    public ImmutableList<Type> getPossibleTypes() {
        return typeReasoner.inferPossibleTypes(this, new ConceptMap());
    }

    @Override
    public List<Atom> atomOptions(ConceptMap sub) {
        return typeReasoner.inferPossibleTypes(this, sub).stream()
                .map(this::addType)
                .sorted(Comparator.comparing(Atom::isRuleResolvable))
                .collect(Collectors.toList());
    }

    @Override
    public Unifier getUnifier(Atom parentAtom, UnifierType unifierType) {
        //in general this <= parent, so no specialisation viable
        if (this.getClass() != parentAtom.getClass()) return UnifierImpl.nonExistent();
        return super.getUnifier(parentAtom, unifierType);
    }

    @Override
    public Stream<ConceptMap> materialise(){
        ConceptMap substitution = getParentQuery().getSubstitution();
        EntityType entityType = getSchemaConcept().asEntityType();

        Concept foundConcept = substitution.containsVar(getVarName())? substitution.get(getVarName()) : null;
        if (foundConcept != null) return Stream.of(substitution);

        Concept concept = entityType.addEntityInferred();
        return Stream.of(
                AnswerUtil.joinAnswers(substitution, new ConceptMap(ImmutableMap.of(getVarName(), concept))
        ));
    }

    @Override
    public Atom rewriteWithTypeVariable() {
        return create(getVarName(), getPredicateVariable().asReturnedVar(), getTypeId(), this.isDirect(), getParentQuery(), context());
    }

    @Override
    public Atom rewriteToUserDefined(Atom parentAtom) {
        return this.rewriteWithTypeVariable(parentAtom);
    }
}
