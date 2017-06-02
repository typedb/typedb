/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the general form: $varName {isa|sub|plays|relates|has|has-scope} $valueVariable).
 * These correspond to the following respective graql properties:
 * {@link IsaProperty},
 * {@link ai.grakn.graql.internal.pattern.property.SubProperty},
 * {@link ai.grakn.graql.internal.pattern.property.PlaysProperty}
 * {@link ai.grakn.graql.internal.pattern.property.RelatesProperty}
 * {@link ai.grakn.graql.internal.pattern.property.HasResourceTypeProperty}
 * {@link ai.grakn.graql.internal.pattern.property.HasScopeProperty}
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class TypeAtom extends Binary{

    public TypeAtom(VarPatternAdmin pattern, ReasonerQuery par) { this(pattern, null, par);}
    public TypeAtom(VarPatternAdmin pattern, IdPredicate p, ReasonerQuery par) { super(pattern, p, par);}
    public TypeAtom(Var var, Var valueVar, IdPredicate p, ReasonerQuery par){
        this(Graql.var(var).isa(Graql.var(valueVar)).admin(), p, par);
    }
    protected TypeAtom(TypeAtom a) { super(a);}

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        BinaryBase a2 = (BinaryBase) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName());
    }

    @Override
    public String toString(){
        String typeString = (getType() != null? getType().getLabel() : "") + "(" + getVarName() + ")";
        return typeString + getIdPredicates().stream().map(IdPredicate::toString).collect(Collectors.joining(""));
    }

    @Override
    protected ConceptId extractTypeId() {
        return getPredicate() != null? getPredicate().getPredicate() : null;
    }

    @Override
    protected Var extractValueVariableName(VarPatternAdmin var) {
        return var.getProperties().findFirst().get().getInnerVars().findFirst().get().getVarName();
    }

    @Override
    protected void setValueVariable(Var var) {
        super.setValueVariable(var);
        atomPattern = atomPattern.asVar().mapProperty(IsaProperty.class, prop -> new IsaProperty(prop.getType().setVarName(var)));
    }

    @Override
    public Atomic copy(){
        return new TypeAtom(this);
    }

    public Set<TypeAtom> unify(Unifier u){
        Collection<Var> vars = u.get(getVarName());
        Var valueVar = getValueVariable();
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> new TypeAtom(v, valueVar, getPredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getHead().getAtom();
        return this.getType() != null
                //ensure not ontological atom query
                && getPattern().asVar().hasProperty(IsaProperty.class)
                && this.getType().subTypes().contains(ruleAtom.getType());
    }

    @Override
    public boolean isSelectable() {
        ReasonerQueryImpl parent = (ReasonerQueryImpl) getParentQuery();
        return getPredicate() == null
                //type atom corresponding to relation or resource
                || getType() != null && (getType().isResourceType() ||getType().isRelationType())
                //disjoint atom
                || parent.findNextJoinable(this) == null
                || isRuleResolvable();
    }

    @Override
    public boolean isAllowedToFormRuleHead(){
        return getType() != null;
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefinedName() && getType() != null && getType().isRelationType();
    }

    @Override
    public int resolutionPriority(){
        if (priority == Integer.MAX_VALUE) {
            priority = super.resolutionPriority();
            priority += ResolutionStrategy.IS_TYPE_ATOM;
            priority += getType() == null && !isRelation()? ResolutionStrategy.NON_SPECIFIC_TYPE_ATOM : 0;
        }
        return priority;
    }

    @Override
    public Type getType() {
        return getPredicate() != null ?
                getParentQuery().graph().getConcept(getPredicate().getPredicate()) : null;
    }
}

