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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.GraknTx;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import com.google.common.collect.Sets;
import java.util.Set;

/**
 *
 * <p>
 * Factory for reasoner queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerQueries {

    public static ReasonerQueryImpl create(Conjunction<VarPatternAdmin> pattern, GraknTx graph) {
        ReasonerQueryImpl query = new ReasonerQueryImpl(pattern, graph);
        return query.isAtomic()? new ReasonerAtomicQuery(pattern, graph) : query;
    }

    public static ReasonerQueryImpl create(ReasonerQueryImpl q) {
        return q.isAtomic()? new ReasonerAtomicQuery(q) : new ReasonerQueryImpl(q);
    }

    public static ReasonerQueryImpl create(Set<Atomic> as, GraknTx tx){
        boolean isAtomic = as.stream().filter(Atomic::isAtom).map(at -> (Atom) at).count() == 1;
        return isAtomic? new ReasonerAtomicQuery(as, tx) : new ReasonerQueryImpl(as, tx);
    }

    public static ReasonerQueryImpl create(ReasonerQueryImpl q, Answer sub){
        return create(Sets.union(q.getAtoms(), sub.toPredicates(q)), q.tx());
    }

    public static ReasonerAtomicQuery atomic(Conjunction<VarPatternAdmin> pattern, GraknTx graph){
        return new ReasonerAtomicQuery(pattern, graph);
    }

    public static ReasonerAtomicQuery atomic(Atom atom){
        return new ReasonerAtomicQuery(atom);
    }

    public static ReasonerAtomicQuery atomic(ReasonerAtomicQuery q){
        return new ReasonerAtomicQuery(q);
    }

    public static ReasonerAtomicQuery atomic(ReasonerAtomicQuery q, Answer sub){
        return new ReasonerAtomicQuery(Sets.union(q.getAtoms(), sub.toPredicates(q)), q.tx());
    }
}
