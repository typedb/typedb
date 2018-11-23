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

package grakn.core.graql.internal.reasoner.unifier;

import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.query.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.UnifierComparison;
import grakn.core.graql.internal.reasoner.atom.binary.ResourceAtom;
import grakn.core.graql.internal.reasoner.cache.QueryCache;
import grakn.core.graql.internal.reasoner.cache.StructuralCache;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 *
 * <p>
 * Class defining different unifier types.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public enum UnifierType implements UnifierComparison, EquivalenceCoupling {

    /**
     *
     * Exact unifier, requires type and id predicate bindings to match.
     * Used in {@link QueryCache} comparisons.
     * An EXACT unifier between two queries can only exists iff they are alpha-equivalent {@link ReasonerQueryEquivalence}.
     * .
     */
    EXACT {

        @Override
        public ReasonerQueryEquivalence equivalence(){
            return ReasonerQueryEquivalence.AlphaEquivalence;
        }

        @Override
        public boolean inferTypes() { return true; }

        @Override
        public boolean typeExplicitenessCompatibility(Atomic parent, Atomic child) {
            return parent.isDirect() == child.isDirect();
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return (parent == null && child == null)
                    || (parent != null && !areDisjointTypes(parent, child, true));
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence().atomicEquivalence().equivalent(parent, child));
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence().atomicEquivalence().equivalent(parent, child));
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            return isEquivalentCollection(parent, child, this::valueCompatibility);
        }
    },

    /**
     *
     * Similar to the exact one with addition to allowing id predicates to differ.
     * Used in {@link StructuralCache} comparisons.
     * A STRUCTURAL unifier between two queries can only exists iff they are structurally-equivalent {@link ReasonerQueryEquivalence}.
     *
     */
    STRUCTURAL {

        @Override
        public ReasonerQueryEquivalence equivalence(){
            return ReasonerQueryEquivalence.StructuralEquivalence;
        }

        @Override
        public boolean inferTypes() { return false; }

        @Override
        public boolean typeExplicitenessCompatibility(Atomic parent, Atomic child) {
            return parent.isDirect() == child.isDirect();
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return (parent == null && child == null)
                    || (parent != null && !areDisjointTypes(parent, child, true));
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return (parent == null) == (child == null);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence().atomicEquivalence().equivalent(parent, child));
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            return isEquivalentCollection(parent, child, this::valueCompatibility);
        }
    },

    /**
     *
     * Rule unifier, found between queries and rule heads, allows rule heads to be more specific than matched queries.
     * Used in rule matching.
     *
     * If two queries are alpha-equivalent they are rule-unifiable.
     * Rule unification relaxes restrictions of exact unification in that it merely
     * requires an existence of a semantic overlap between the parent and child queries, i. e.
     * the answer set of the child and the parent queries need to have a non-zero intersection.
     *
     * For predicates it corresponds to changing the alpha-equivalence requirement to compatibility.
     *
     * As a result, two queries may be rule-unifiable and not alpha-equivalent, e.q.
     *
     * P: $x has age >= 10
     * Q: $x has age 10
     *
     */
    RULE {
        @Override
        public ReasonerQueryEquivalence equivalence() { return null; }

        @Override
        public boolean inferTypes() { return true; }

        @Override
        public boolean typeExplicitenessCompatibility(Atomic parent, Atomic child) { return true; }

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return child == null || !areDisjointTypes(parent, child, false);
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || parent.isAlphaEquivalent(child);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || child.isCompatibleWith(parent);
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            return super.attributeValueCompatibility(parent, child)
                    //inter-vp compatibility
                    && (
                    parent.isEmpty()
                            || child.stream().allMatch(cp -> parent.stream().allMatch(pp -> valueCompatibility(pp, cp)))
            );
        }

        @Override
        public boolean attributeCompatibility(ReasonerQuery parent, ReasonerQuery child, Var parentVar, Var childVar){
            Map<SchemaConcept, ResourceAtom> parentRes = new HashMap<>();
            parent.getAtoms(ResourceAtom.class).filter(at -> at.getVarName().equals(parentVar)).forEach(r -> parentRes.put(r.getSchemaConcept(), r));
            Map<SchemaConcept, ResourceAtom> childRes = new HashMap<>();
            child.getAtoms(ResourceAtom.class).filter(at -> at.getVarName().equals(childVar)).forEach(r -> childRes.put(r.getSchemaConcept(), r));
            return childRes.values().stream()
                    .allMatch(r -> !parentRes.containsKey(r.getSchemaConcept()) || r.isUnifiableWith(parentRes.get(r.getSchemaConcept())));
        }
    },

    /**
     * Unifier type used to determine whether two queries are in a subsumption relation.
     * Subsumption can be regarded as a stricter version of the semantic overlap requirement seen in RULE {@link UnifierType}.
     * Defining queries Q and P and their respective answer sets A(Q) and A(P) we say that:
     *
     * Q subsumes P iff
     * P >= Q (Q specialises P) iff
     * A(Q) is a subset of A(P)
     *
     * Subsumption relation is NOT symmetric.
     *
     */
    SUBSUMPTIVE {
        @Override
        public ReasonerQueryEquivalence equivalence() { return null; }

        @Override
        public boolean inferTypes() { return false; }

        @Override
        public boolean typeExplicitenessCompatibility(Atomic parent, Atomic child) {
            return !parent.isDirect()
                    || (parent.isDirect() == child.isDirect());
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return (child == null && parent == null)
                    || (child != null && parent == null)
                    || (child != null && parent.subs().anyMatch(child::equals));
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return parent == null
                    || child != null && child.subsumes(parent);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return parent == null
                    || child != null && child.subsumes(parent);
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            //check both ways to eliminate contradictions
            boolean parentToChild = parent.stream().allMatch(pp -> child.stream().anyMatch(cp -> cp.subsumes(pp)));
            boolean childToParent = child.stream().allMatch(cp -> parent.stream().anyMatch(pp -> cp.isCompatibleWith(pp)));
            return super.attributeValueCompatibility(parent, child)
                    && (parent.isEmpty()
                    || (!child.isEmpty() && parentToChild && childToParent));
        }

        @Override
        public boolean attributeCompatibility(ReasonerQuery parent, ReasonerQuery child, Var parentVar, Var childVar){
            Map<SchemaConcept, ResourceAtom> parentRes = new HashMap<>();
            parent.getAtoms(ResourceAtom.class).filter(at -> at.getVarName().equals(parentVar)).forEach(r -> parentRes.put(r.getSchemaConcept(), r));
            Map<SchemaConcept, ResourceAtom> childRes = new HashMap<>();
            child.getAtoms(ResourceAtom.class).filter(at -> at.getVarName().equals(childVar)).forEach(r -> childRes.put(r.getSchemaConcept(), r));
            return childRes.values().stream()
                    .allMatch(r -> !parentRes.containsKey(r.getSchemaConcept()) || r.isUnifiableWith(parentRes.get(r.getSchemaConcept())));
        }
    }
}