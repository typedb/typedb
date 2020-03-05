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

package grakn.core.graql.reasoner.atom;

import com.google.common.base.Equivalence;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.kb.graql.reasoner.atom.Atomic;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;


/**
 *
 * <p>
 * Static class defining different equivalence comparisons for Atomics :
 *
 * - Equality: two Atomics are equal if they are equal including all their corresponding variables.
 *
 * - Alpha-equivalence: two Atomics are alpha-equivalent if they are equal up to the choice of free variables.
 *
 * - Structural equivalence:
 * Two atomics are structurally equivalent if they are equal up to the choice of free variables and partial substitutions (id predicates IdPredicate).
 * Hence:
 * - any two IdPredicates are structurally equivalent
 * - structural equivalence check on queries is done by looking at Atoms. As a result:
 *   * connected predicates are assessed together with atoms they are connected to
 *   * dangling predicates are ignored
 *
 * </p>
 *
 *
 */
public abstract class AtomicEquivalence extends Equivalence<Atomic> {

    abstract public String name();

    public static <B extends Atomic, S extends B> boolean equivalence(Collection<S> a1, Collection<S> a2, Equivalence<B> equiv){
        return ReasonerUtils.isEquivalentCollection(a1, a2, equiv);
    }

    public static <B extends Atomic, S extends B> int equivalenceHash(Stream<S> atoms, Equivalence<B> equiv){
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        atoms.forEach(atom -> hashes.add(equiv.hash(atom)));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    public static <B extends Atomic, S extends B> int equivalenceHash(Collection<S> atoms, Equivalence<B> equiv){
        return equivalenceHash(atoms.stream(), equiv);
    }

    public final static AtomicEquivalence Equality = new AtomicEquivalence(){

        @Override
        public String name(){ return "Equality";}

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.equals(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.hashCode();
        }
    };

    public final static AtomicEquivalence AlphaEquivalence = new AtomicEquivalence(){

        @Override
        public String name(){ return "AlphaEquivalence";}

        @Override
        protected boolean doEquivalent(Atomic a, Atomic b) {
            return a.isAlphaEquivalent(b);
        }

        @Override
        protected int doHash(Atomic a) {
            return a.alphaEquivalenceHashCode();
        }
    };

    public final static AtomicEquivalence StructuralEquivalence = new AtomicEquivalence(){

        @Override
        public String name(){ return "StructuralEquivalence";}

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
