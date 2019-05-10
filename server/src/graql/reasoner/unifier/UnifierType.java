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

package grakn.core.graql.reasoner.unifier;

import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.cache.QueryCache;
import grakn.core.graql.reasoner.cache.StructuralCache;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.server.kb.concept.ConceptUtils;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static grakn.core.graql.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 * Class defining different unifier types.
 */
public enum UnifierType implements UnifierComparison, EquivalenceCoupling {

    /**
     * Exact unifier, requires type and id predicate bindings to match.
     * Used in {@link QueryCache} comparisons.
     * An EXACT unifier between two queries can only exists iff they are alpha-equivalent {@link ReasonerQueryEquivalence}.
     * .
     */
    EXACT {
        @Override
        public ReasonerQueryEquivalence equivalence() {
            return ReasonerQueryEquivalence.AlphaEquivalence;
        }

        @Override
        public boolean inferTypes() { return true; }

        @Override
        public boolean inferValues() { return false; }

        @Override
        public boolean typeDirectednessCompatibility(Atomic parent, Atomic child) {
            return parent.isDirect() == child.isDirect();
        }

        @Override
        public boolean roleCompatibility(Role parent, Role child) {
            return parent == null && child == null
                    || parent != null && parent.equals(child);
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Variable var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(Set<? extends SchemaConcept> parentTypes, Set<? extends SchemaConcept> childTypes) {
            return super.typeCompatibility(parentTypes, childTypes)
                    && parentTypes.equals(childTypes);
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
        public boolean predicateCompatibility(Set<Atomic> parent, Set<Atomic> child, BiFunction<Atomic, Atomic, Boolean> comparison) {
            return isEquivalentCollection(parent, child, comparison);
        }

    },

    /**
     * Similar to the exact one with addition to allowing id predicates to differ.
     * Used in {@link StructuralCache} comparisons.
     * A STRUCTURAL unifier between two queries can only exists iff they are structurally-equivalent {@link ReasonerQueryEquivalence}.
     */
    STRUCTURAL {
        @Override
        public ReasonerQueryEquivalence equivalence() {
            return ReasonerQueryEquivalence.StructuralEquivalence;
        }

        @Override
        public boolean inferTypes() { return false; }

        @Override
        public boolean inferValues() { return false; }

        @Override
        public boolean typeDirectednessCompatibility(Atomic parent, Atomic child) {
            return parent.isDirect() == child.isDirect();
        }

        @Override
        public boolean roleCompatibility(Role parent, Role child) {
            return parent == null && child == null
                    || parent != null && parent.equals(child);
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Variable var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(Set<? extends SchemaConcept> parentTypes, Set<? extends SchemaConcept> childTypes) {
            return super.typeCompatibility(parentTypes, childTypes)
                    && parentTypes.equals(childTypes);
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
        public boolean predicateCompatibility(Set<Atomic> parent, Set<Atomic> child, BiFunction<Atomic, Atomic, Boolean> comparison) {
            return isEquivalentCollection(parent, child, comparison);
        }
    },

    /**
     * Rule unifier, found between queries and rule heads, allows rule heads to be more general than matched queries.
     * Used in rule matching. The general condition of the child query C (rule head) and parent query P that needs to be satisfied is:
     *
     * C >= P,
     *
     * i. e. parent specialises the child.
     *
     * If two queries are alpha-equivalent they are rule-unifiable.
     * Rule unification relaxes restrictions of exact unification in that it merely
     * requires an existence of a semantic overlap between the parent and child queries, i. e.
     * the answer set of the child and the parent queries need to have a non-zero intersection.
     *
     * For predicates it corresponds to changing the alpha-equivalence requirement to compatibility.
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
        public boolean inferValues() { return true; }

        @Override
        public boolean typeDirectednessCompatibility(Atomic parent, Atomic child) { return true; }

        @Override
        public boolean roleCompatibility(Role parent, Role child) {
            return parent == null || parent.subs().anyMatch(sub -> sub.equals(child));
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Variable var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(Set<? extends SchemaConcept> parentTypes, Set<? extends SchemaConcept> childTypes) {
            return super.typeCompatibility(parentTypes, childTypes)
                && (childTypes.isEmpty() || !ConceptUtils.areDisjointTypeSets(parentTypes, childTypes, false));
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || child.isAlphaEquivalent(parent);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || child.isCompatibleWith(parent);
        }

        @Override
        public boolean predicateCompatibility(Set<Atomic> parent, Set<Atomic> child, BiFunction<Atomic, Atomic, Boolean> comparison) {
            return super.predicateCompatibility(parent, child, comparison)
                    //inter-vp compatibility
                    && (
                    parent.isEmpty()
                            || child.stream().allMatch(cp -> parent.stream().allMatch(pp -> comparison.apply(pp, cp)))
            );
        }

        @Override
        public boolean attributeCompatibility(ReasonerQuery parent, ReasonerQuery child, Variable parentVar, Variable childVar) {
            Map<SchemaConcept, AttributeAtom> parentRes = new HashMap<>();
            parent.getAtoms(AttributeAtom.class).filter(at -> at.getVarName().equals(parentVar)).forEach(r -> parentRes.put(r.getSchemaConcept(), r));
            Map<SchemaConcept, AttributeAtom> childRes = new HashMap<>();
            child.getAtoms(AttributeAtom.class).filter(at -> at.getVarName().equals(childVar)).forEach(r -> childRes.put(r.getSchemaConcept(), r));
            return childRes.values().stream()
                    .allMatch(r -> !parentRes.containsKey(r.getSchemaConcept()) || r.isUnifiableWith(parentRes.get(r.getSchemaConcept())));
        }
    },

    /**
     * Unifier type used to determine whether two queries are in a subsumption relation.
     * Subsumption can be regarded as a stricter version of the semantic overlap requirement seen in RULE {@link UnifierType}.
     * Defining queries C and P and their respective answer sets A(C) and A(P) we say that a subsumptive unifier between child
     * and parent exists if:
     *
     * C <= P,
     *
     * i.e. C specialises P (C subsumes P) and A(C) is a subset of A(P).
     *
     * As a result, to relate it with the RULE unifier. We can say that if there exists a RULE unifier between child and
     * parent, i. e. C >= P holds, then there exists a SUBSUMPTIVE unifier between parent and child.
     *
     * Subsumption relation is NOT symmetric in general. The only case when it is symmetric is when parent and child
     * queries are alpha-equivalent.
     */
    SUBSUMPTIVE {

        @Override
        public ReasonerQueryEquivalence equivalence() { return null; }

        @Override
        public boolean inferTypes() { return false; }

        @Override
        public boolean inferValues() { return true; }

        @Override
        public boolean allowsNonInjectiveMappings() { return false; }

        @Override
        public boolean typeDirectednessCompatibility(Atomic parent, Atomic child) {
            //we require equal directedness as we can't always check the type in the answer (e.g. if we have a relation without rel var)
            return (parent.isDirect() == child.isDirect());
        }

        @Override
        public boolean roleCompatibility(Role parent, Role child) {
            return parent == null || parent.subs().anyMatch(sub -> sub.equals(child));
        }

        @Override
        public boolean typePlayability(ReasonerQuery query, Variable var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(Set<? extends SchemaConcept> parentTypes, Set<? extends SchemaConcept> childTypes) {
            return super.typeCompatibility(parentTypes, childTypes)
                    && (parentTypes.stream().allMatch(t -> t.subs().anyMatch(childTypes::contains)))
                    && !ConceptUtils.areDisjointTypeSets(parentTypes, childTypes, false);
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
        public boolean predicateCompatibility(Set<Atomic> parent, Set<Atomic> child, BiFunction<Atomic, Atomic, Boolean> comparison) {
            //check both ways to eliminate contradictions
            boolean parentToChild = parent.stream().allMatch(pp -> child.stream().anyMatch(cp -> cp.subsumes(pp)));
            boolean childToParent = child.stream().allMatch(cp -> parent.stream().anyMatch(pp -> cp.isCompatibleWith(pp)));
            return super.predicateCompatibility(parent, child, comparison)
                    && (parent.isEmpty()
                    || (!child.isEmpty() && parentToChild && childToParent));
        }

        @Override
        public boolean attributeCompatibility(ReasonerQuery parent, ReasonerQuery child, Variable parentVar, Variable childVar) {
            Map<SchemaConcept, AttributeAtom> parentRes = new HashMap<>();
            parent.getAtoms(AttributeAtom.class).filter(at -> at.getVarName().equals(parentVar)).forEach(r -> parentRes.put(r.getSchemaConcept(), r));
            Map<SchemaConcept, AttributeAtom> childRes = new HashMap<>();
            child.getAtoms(AttributeAtom.class).filter(at -> at.getVarName().equals(childVar)).forEach(r -> childRes.put(r.getSchemaConcept(), r));
            return childRes.values().stream()
                    .allMatch(r -> !parentRes.containsKey(r.getSchemaConcept()) || r.isUnifiableWith(parentRes.get(r.getSchemaConcept())));
        }
    }
}