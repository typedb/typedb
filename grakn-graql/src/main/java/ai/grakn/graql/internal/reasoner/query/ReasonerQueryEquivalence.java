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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
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
 * Static class defining different equivalence comparisons for reasoner queries ({@link ReasonerQuery}):
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
public abstract class ReasonerQueryEquivalence extends Equivalence<ReasonerQuery> {

    private static <T extends Atomic> boolean equivalence(ReasonerQuery q1, ReasonerQuery q2, Class<T> atomType, BiFunction<T, T, Boolean> equivalenceFunction) {
        //NB: this check is too simple for general queries - variable binding patterns are not recognised
        Set<T> atoms = q1.getAtoms(atomType).collect(Collectors.toSet());
        Set<T> otherAtoms = q2.getAtoms(atomType).collect(Collectors.toSet());
        return ReasonerUtils.isEquivalentCollection(atoms, otherAtoms, equivalenceFunction);
    }

    private static <T extends Atomic> int equivalenceHash(ReasonerQuery q, Class<T> atomType, Function<T, Integer> hashFunction) {
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        q.getAtoms(atomType).forEach(atom -> hashes.add(hashFunction.apply(atom)));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

    public final static Equivalence<ReasonerQuery> Equality = new ReasonerQueryEquivalence(){

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atomic.class, Atomic::equals);
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atomic.class, Atomic::hashCode);
        }
    };

    public final static Equivalence<ReasonerQuery> AlphaEquivalence = new ReasonerQueryEquivalence(){

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atomic.class, Atomic::isAlphaEquivalent);
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atomic.class, Atomic::alphaEquivalenceHashCode);
        }
    };

    public final static Equivalence<ReasonerQuery> StructuralEquivalence = new ReasonerQueryEquivalence(){

        @Override
        protected boolean doEquivalent(ReasonerQuery q1, ReasonerQuery q2) {
            return equivalence(q1, q2, Atom.class, Atomic::isStructurallyEquivalent);
        }

        @Override
        protected int doHash(ReasonerQuery q) {
            return equivalenceHash(q, Atom.class, Atomic::structuralEquivalenceHashCode);
        }
    };
}
