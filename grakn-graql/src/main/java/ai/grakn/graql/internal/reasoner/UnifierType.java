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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UnifierComparison;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.isEquivalentCollection;

/**
 *
 * <p>
 * Class defining different unifier types.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public enum UnifierType implements UnifierComparison {

    /**
     *
     * Exact unifier, requires type and id predicate bindings to match.
     * Used in {@link ai.grakn.graql.internal.reasoner.cache.QueryCache} comparisons.
     * An EXACT unifier between two queries can only exists iff they are alpha-equivalent {@link ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence}.
     * .
     */
    EXACT {
        private final ReasonerQueryEquivalence equivalence = ReasonerQueryEquivalence.AlphaEquivalence;

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return !areDisjointTypes(parent, child);
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence.atomicEquivalence().equivalent(parent, child));
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence.atomicEquivalence().equivalent(parent, child));
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            return isEquivalentCollection(parent, child, this::valueCompatibility);
        }
    },

    /**
     *
     * Similar to the exact one with addition to allowing id predicates to differ.
     * Used in {@link ai.grakn.graql.internal.reasoner.cache.StructuralCache} comparisons.
     * A STRUCTURAL unifier between two queries can only exists iff they are structurally-equivalent {@link ai.grakn.graql.internal.reasoner.query.ReasonerQueryEquivalence}.
     *
     */
    STRUCTURAL {

        private final ReasonerQueryEquivalence equivalence = ReasonerQueryEquivalence.StructuralEquivalence;

        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return ((child == null ) == (parent == null)) && !areDisjointTypes(parent, child);
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return (parent == null) == (child == null);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return (parent == null && child == null)
                    || (parent != null && equivalence.atomicEquivalence().equivalent(parent, child));
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
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(SchemaConcept parent, SchemaConcept child) {
            return child == null || !areDisjointTypes(parent, child);
        }

        @Override
        public boolean idCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || parent.isCompatibleWith(child);
        }

        @Override
        public boolean valueCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || parent.isCompatibleWith(child);
        }

        @Override
        public boolean attributeValueCompatibility(Set<Atomic> parent, Set<Atomic> child) {
            return parent.isEmpty() || child.stream().allMatch(cp -> parent.stream().anyMatch(pp -> valueCompatibility(pp, cp)));
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