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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.ResolutionStrategy;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
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

    public TypeAtom(VarAdmin pattern, ReasonerQuery par) { this(pattern, null, par);}
    public TypeAtom(VarAdmin pattern, IdPredicate p, ReasonerQuery par) { super(pattern, p, par);}
    protected TypeAtom(TypeAtom a) { super(a);}

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
    protected VarName extractValueVariableName(VarAdmin var) {
        return var.getProperties().findFirst().get().getInnerVars().findFirst().get().getVarName();
    }

    @Override
    protected void setValueVariable(VarName var) {
        super.setValueVariable(var);
        atomPattern = atomPattern.asVar().mapProperty(IsaProperty.class, prop -> new IsaProperty(prop.getType().setVarName(var)));
    }

    @Override
    public Atomic copy(){
        return new TypeAtom(this);
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
                || (!(parent instanceof ReasonerAtomicQuery) && parent.findNextJoinable(this) == null)
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
        int priority = super.resolutionPriority();
        priority += ResolutionStrategy.IS_TYPE_ATOM;
        priority += getType() == null? ResolutionStrategy.NON_SPECIFIC_TYPE_ATOM : 0;
        return priority;
    }

    @Override
    public Type getType() {
        return getPredicate() != null ?
                getParentQuery().graph().getConcept(getPredicate().getPredicate()) : null;
    }
}

