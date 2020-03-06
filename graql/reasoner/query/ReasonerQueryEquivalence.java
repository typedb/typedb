/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.reasoner.query;

import com.google.common.base.Equivalence;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;

import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Static class defining different equivalence comparisons for reasoner queries (ReasonerQuery).
 *
 * </p>
 *
 */
public abstract class ReasonerQueryEquivalence extends Equivalence<ReasonerQuery> {

    abstract public AtomicEquivalence atomicEquivalence();
    public String name(){ return atomicEquivalence().name();}

    private static <B extends Atomic, S extends B> boolean equivalence(ReasonerQuery q1, ReasonerQuery q2, Class<S> atomType, Equivalence<B> equiv) {
        //NB: this check is too simple for general queries - variable binding patterns are not recognised
        Set<S> atoms = q1.getAtoms(atomType).collect(Collectors.toSet());
        Set<S> otherAtoms = q2.getAtoms(atomType).collect(Collectors.toSet());
        return AtomicEquivalence.equivalence(atoms, otherAtoms, equiv);
    }

    private static <B extends Atomic, S extends B> int equivalenceHash(ReasonerQuery q, Class<S> atomType, Equivalence<B> equiv) {
        return AtomicEquivalence.equivalenceHash(q.getAtoms(atomType), equiv);
    }

    /**
     * Equality - two queries are equal if they contain the same Atomics of which all corresponding pairs are equal.
     */
    public final static ReasonerQueryEquivalence Equality = new ReasonerQueryEquivalence(){

        @Override
        public AtomicEquivalence atomicEquivalence() { return AtomicEquivalence.Equality; }

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atomic.class, atomicEquivalence());
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atomic.class, atomicEquivalence());
        }
    };

    /**
     * Alpha equivalence - two queries are alpha-equivalent if they are equal up to the choice of free variables.
     * NB: two queries are alpha-equivalent iff their answer sets are equal.
     */
    public final static ReasonerQueryEquivalence AlphaEquivalence = new ReasonerQueryEquivalence(){

        @Override
        public AtomicEquivalence atomicEquivalence() { return AtomicEquivalence.AlphaEquivalence; }

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atomic.class, atomicEquivalence());
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atomic.class, atomicEquivalence());
        }
    };

    /**
     * Structural equivalence - two queries are structurally equivalent if they are equal up to the choice of free variables and partial substitutions (id predicates).
     * * NB: two queries are structurally-equivalent iff after id transformation, their answer sets are equal.
     */
    public final static ReasonerQueryEquivalence StructuralEquivalence = new ReasonerQueryEquivalence(){

        @Override
        public AtomicEquivalence atomicEquivalence() { return AtomicEquivalence.StructuralEquivalence; }

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atom.class, atomicEquivalence());
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atom.class, atomicEquivalence());
        }
    };
}
