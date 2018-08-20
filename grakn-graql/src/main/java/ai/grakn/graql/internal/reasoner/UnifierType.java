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
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.areDisjointTypes;

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
     * Used in {@link ai.grakn.graql.internal.reasoner.cache.QueryCache} comparisons
     * .
     */
    EXACT {
        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(Type parent, Type child) {
            return !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return parent == null || parent.isCompatibleWith(child);
        }

    },

    /**
     *
     * Rule unifier, found between queries and rule heads, allows rule heads to be more specific than matched queries.
     * Used in rule matching.
     *
     */
    RULE {
        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean typeCompatibility(Type parent, Type child) {
            return child == null || !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || parent.isCompatibleWith(child);
        }

        @Override
        public boolean attributeCompatibility(Atomic parent, Atomic child, Var parentVar, Var childVar){
            Map<SchemaConcept, ResourceAtom> parentRes = new HashMap<>();
            parent.getParentQuery().getAtoms(ResourceAtom.class)
                    .filter(at -> at.getVarName().equals(parentVar))
                    .filter(ResourceAtom::isSpecific)
                    .forEach(r -> parentRes.put(r.getSchemaConcept(), r));
            Map<SchemaConcept, ResourceAtom> childRes = new HashMap<>();
            child.getParentQuery().getAtoms(ResourceAtom.class)
                    .filter(at -> at.getVarName().equals(childVar))
                    .filter(ResourceAtom::isSpecific)
                    .forEach(r -> childRes.put(r.getSchemaConcept(), r));
            return childRes.values().stream()
                    .allMatch(r -> !parentRes.containsKey(r.getSchemaConcept()) || r.isUnifiableWith(parentRes.get(r.getSchemaConcept())));
        }
    },

    /**
     *
     * Similar to rule one with addition to allowing id predicates to differ.
     * Used in {@link ai.grakn.graql.internal.reasoner.cache.StructuralCache} comparisons.
     *
     */
    STRUCTURAL {
        @Override
        public boolean typePlayability(ReasonerQuery query, Var var, Type type) {
            return true;
        }

        @Override
        public boolean typeCompatibility(Type parent, Type child) {
            return child == null || !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return (parent == null) == (child == null);
        }

    }
}