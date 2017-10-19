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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UnifierComparison;

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
     * Exact unifier, requires type and id predicate bindings to match.
     */
    EXACT {
        @Override
        public boolean typeCompatibility(ReasonerQuery query, Var var, SchemaConcept type) {
            return true;
        }

        @Override
        public boolean schemaConceptComparison(SchemaConcept parent, SchemaConcept child) {
            return !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return parent == null || parent.isCompatibleWith(child);
        }
    },

    /**
     * Rule unifier, found between queries and rule heads, allows rule heads to be more specific than matched queries.
     */
    RULE {
        @Override
        public boolean typeCompatibility(ReasonerQuery query, Var var, SchemaConcept type) {
            return query.isTypeRoleCompatible(var, type);
        }

        @Override
        public boolean schemaConceptComparison(SchemaConcept parent, SchemaConcept child) {
            return child == null || !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return child == null || parent == null || parent.isCompatibleWith(child);
        }
    },

    /**
     * Similar to rule one with addition to allowing id predicates to differ.
     */
    STRUCTURAL {
        @Override
        public boolean typeCompatibility(ReasonerQuery query, Var var, SchemaConcept type) {
            return true;
        }

        @Override
        public boolean schemaConceptComparison(SchemaConcept parent, SchemaConcept child) {
            return child == null || !areDisjointTypes(parent, child);
        }

        @Override
        public boolean atomicCompatibility(Atomic parent, Atomic child) {
            return (parent == null) == (child == null);
        }
    }
}