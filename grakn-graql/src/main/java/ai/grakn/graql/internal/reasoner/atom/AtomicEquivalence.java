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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.admin.Atomic;
import com.google.common.base.Equivalence;


/**
 *
 * <p>
 * Static class defining different equivalence comparisons for atomics({@link Atomic}):
 *
 * - alpha equivalence - two atomics are alpha-equivalent if they are equal up to the choice of free variables
 *
 * - structural equivalence - two atomics are structurally equivalent if they are equal up to the choice of free variables and partial substitutions (id predicates {@link ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate})
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomicEquivalence extends Equivalence<Atomic> {

    public final static Equivalence<Atomic> Equality = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.equals(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.hashCode();
        }
    };

    public final static Equivalence<Atomic> AlphaEquivalence = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.isAlphaEquivalent(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.alphaEquivalenceHashCode();
        }
    };

    public final static Equivalence<Atomic> StructuralEquivalence = new AtomicEquivalence(){

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.isStructurallyEquivalent(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.structuralEquivalenceHashCode();
        }
    };
}
