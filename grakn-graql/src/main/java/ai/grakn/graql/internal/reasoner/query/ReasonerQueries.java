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

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;

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

    public static ReasonerQueryImpl create(Conjunction<VarAdmin> pattern, GraknGraph graph) {
        ReasonerQueryImpl query = new ReasonerQueryImpl(pattern, graph);
        return query.isAtomic()? new ReasonerAtomicQuery(pattern, graph) : query;
    }

    public static ReasonerQueryImpl create(ReasonerQueryImpl q) {
        return q.isAtomic()? new ReasonerAtomicQuery(q) : new ReasonerQueryImpl(q);
    }

    public static ReasonerAtomicQuery atomic(Conjunction<VarAdmin> pattern, GraknGraph graph){
        return new ReasonerAtomicQuery(pattern, graph);
    }

    public static ReasonerAtomicQuery atomic(Atom atom){
        return new ReasonerAtomicQuery(atom);
    }

    public static ReasonerAtomicQuery atomic(ReasonerQueryImpl q){
        return new ReasonerAtomicQuery(q);
    }

    /**
     * construct Q' = Q \ atom
     * @param q entry query
     * @param atom atom to be removed
     * @return Q'
     */
    static ReasonerQueryImpl prime(ReasonerQueryImpl q, Atom atom){
        ReasonerQueryImpl query = q.removeAtom(atom);
        return query.isAtomic()? new ReasonerAtomicQuery(query) : query;
    }
}
