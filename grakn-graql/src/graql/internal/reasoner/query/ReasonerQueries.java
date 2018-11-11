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

package grakn.core.graql.internal.reasoner.query;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

/**
 *
 * <p>
 * Factory for reasoner queries.
 * </p>
 *
 *
 */
public class ReasonerQueries {

    /**
     * create a reasoner query from a conjunctive pattern with types inferred
     * @param pattern conjunctive pattern defining the query
     * @param tx corresponding transaction
     * @return reasoner query constructed from provided conjunctive pattern
     */
    public static ReasonerQueryImpl create(Conjunction<VarPatternAdmin> pattern, EmbeddedGraknTx<?> tx) {
        ReasonerQueryImpl query = new ReasonerQueryImpl(pattern, tx).inferTypes();
        return query.isAtomic()?
                new ReasonerAtomicQuery(query.getAtoms(), tx) :
                query;
    }

    /**
     * create a reasoner query from provided set of atomics
     * @param as set of atomics that define the query
     * @param tx corresponding transaction
     * @return reasoner query defined by the provided set of atomics
     */
    public static ReasonerQueryImpl create(Set<Atomic> as, EmbeddedGraknTx<?> tx){
        boolean isAtomic = as.stream().filter(Atomic::isSelectable).count() == 1;
        return isAtomic?
                new ReasonerAtomicQuery(as, tx).inferTypes() :
                new ReasonerQueryImpl(as, tx).inferTypes();
    }

    /**
     * create a reasoner query from provided list of atoms
     * NB: atom constraints (types and predicates, if any) will be included in the query
     * @param as list of atoms that define the query
     * @param tx corresponding transaction
     * @return reasoner query defined by the provided list of atoms together with their constraints (types and predicates, if any)
     */
    public static ReasonerQueryImpl create(List<Atom> as, EmbeddedGraknTx<?> tx){
        boolean isAtomic = as.size() == 1;
        return isAtomic?
                new ReasonerAtomicQuery(Iterables.getOnlyElement(as)).inferTypes() :
                new ReasonerQueryImpl(as, tx).inferTypes();
    }

    /**
     * create a reasoner query by combining an existing query and a substitution
     * @param q base query for substitution to be attached
     * @param sub (partial) substitution
     * @return reasoner query with the substitution contained in the query
     */
    public static ReasonerQueryImpl create(ReasonerQueryImpl q, ConceptMap sub){
        return q.withSubstitution(sub).inferTypes();
    }

    /**
     * @param pattern conjunctive pattern defining the query
     * @param tx corresponding transaction
     * @return atomic query defined by the provided pattern with inferred types
     */
    public static ReasonerAtomicQuery atomic(Conjunction<VarPatternAdmin> pattern, EmbeddedGraknTx<?> tx){
        return new ReasonerAtomicQuery(pattern, tx).inferTypes();
    }

    /**
     * create an atomic query from the provided atom
     * NB: atom constraints (types and predicates, if any) will be included in the query
     * @param atom defining the query
     * @return atomic query defined by the provided atom together with its constraints (types and predicates, if any)
     */
    public static ReasonerAtomicQuery atomic(Atom atom){
        return new ReasonerAtomicQuery(atom).inferTypes();
    }

    /**
     * create an atomic query copy from the provided query with the types inferred
     * @param q query to be copied
     * @return copied atomic query with inferred types
     */
    public static ReasonerAtomicQuery atomic(ReasonerAtomicQuery q){
        return new ReasonerAtomicQuery(q).inferTypes();
    }

    /**
     * create an atomic query by combining an existing atomic query and a substitution
     * @param q base query for substitution to be attached
     * @param sub (partial) substitution
     * @return atomic query with the substitution contained in the query
     */
    public static ReasonerAtomicQuery atomic(ReasonerAtomicQuery q, ConceptMap sub){
        return q.withSubstitution(sub).inferTypes();
    }
}
