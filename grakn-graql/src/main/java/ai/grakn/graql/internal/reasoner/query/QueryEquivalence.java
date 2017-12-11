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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import com.google.common.base.Equivalence;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 *
 * <p>
 * Static class defining different equivalence comparisons for reasoner queries ({@link ReasonerQueryImpl}):
 *
 * - alpha equivalence - two queries are alpha-equivalent if they are equal up to the choice of free variables
 *
 * - structural equivalence - two queries are structurally equivalent if they are equal up to the choice of free variables and partial substitutions (id predicates)
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class QueryEquivalence extends Equivalence<ReasonerQueryImpl> {

    private static boolean equivalence(ReasonerQueryImpl q1, ReasonerQueryImpl q2, BiFunction<Atom, Atom, Boolean> equivalenceFunction) {
        if(q1.getAtoms().size() != q2.getAtoms().size()) return false;
        Set<Atom> atoms = q1.getAtoms(Atom.class).collect(Collectors.toSet());
        for (Atom atom : atoms){
            if(!q2.containsEquivalentAtom(atom, equivalenceFunction)){
                return false;
            }
        }
        return true;
    }

    private static <T extends Atomic> int equivalenceHash(ReasonerQueryImpl q, Class<T> atomType, Function<Atomic, Integer> hashFunction) {
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        q.getAtoms(atomType).forEach(atom -> hashes.add(hashFunction.apply(atom)));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    public final static Equivalence<ReasonerQueryImpl> AlphaEquivalence = new QueryEquivalence(){

        @Override
        protected boolean doEquivalent(ReasonerQueryImpl q1, ReasonerQueryImpl q2) {
            return equivalence(q1, q2, Atomic::isAlphaEquivalent);
        }

        @Override
        protected int doHash(ReasonerQueryImpl q) {
            return equivalenceHash(q, Atomic.class, Atomic::alphaEquivalenceHashCode);
        }
    };

    public final static Equivalence<ReasonerQueryImpl> StructuralEquivalence = new QueryEquivalence(){

        @Override
        protected boolean doEquivalent(ReasonerQueryImpl q1, ReasonerQueryImpl q2) {
            return equivalence(q1, q2, Atomic::isStructurallyEquivalent);
        }

        @Override
        protected int doHash(ReasonerQueryImpl q) {
            return equivalenceHash(q, Atom.class, Atomic::structuralEquivalenceHashCode);
        }
    };
}
