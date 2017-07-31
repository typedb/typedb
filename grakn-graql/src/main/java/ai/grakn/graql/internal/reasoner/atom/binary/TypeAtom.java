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

import ai.grakn.concept.OntologyConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the general form:
 *
 * {isa|sub|plays|relates|has|has-scope}($varName, $predicateVariable)
 *
 * Type atoms correspond to the following respective graql properties:
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
public abstract class TypeAtom extends Binary{

    protected TypeAtom(VarPatternAdmin pattern, Var predicateVar, @Nullable IdPredicate p, ReasonerQuery par) {
        super(pattern, predicateVar, p, par);}
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
        Binary a2 = (Binary) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName());
    }

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean isRuleApplicable(InferenceRule child) {
        Atom ruleAtom = child.getHead().getAtom();
        return this.getOntologyConcept() != null
                //ensure not ontological atom query
                && getPattern().asVar().hasProperty(IsaProperty.class)
                && this.getOntologyConcept().subs().contains(ruleAtom.getOntologyConcept());
    }

    @Override
    public boolean isSelectable() {
        return getPredicate() == null
                //disjoint atom
                || !this.getNeighbours().findFirst().isPresent()
                || isRuleResolvable();
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefinedName() && getOntologyConcept() != null && getOntologyConcept().isRelationType();
    }

    @Override
    public int computePriority(Set<Var> subbedVars){
        int priority = super.computePriority(subbedVars);
        priority += ResolutionPlan.IS_TYPE_ATOM;
        priority += getOntologyConcept() == null && !isRelation()? ResolutionPlan.NON_SPECIFIC_TYPE_ATOM : 0;
        return priority;
    }

    @Nullable
    @Override
    public OntologyConcept getOntologyConcept() {
        return getPredicate() != null ?
                getParentQuery().graph().getConcept(getPredicate().getPredicate()) : null;
    }

    public abstract Set<TypeAtom> unify(Unifier u);
}

