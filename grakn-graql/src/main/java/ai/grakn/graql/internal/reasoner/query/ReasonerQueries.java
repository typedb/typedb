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

<<<<<<< HEAD
import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Atomic;
=======
import ai.grakn.GraknTx;
>>>>>>> af2276f104a6e1cf059ff5cdba6152b2211e523d
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import java.util.Set;
import java.util.stream.Collectors;

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

<<<<<<< HEAD
    public static ReasonerQueryImpl create(Set<Atomic> as, GraknGraph graph){
        Set<Atom> atoms = as.stream().filter(Atomic::isAtom).map(at -> (Atom) at).collect(Collectors.toSet());
        return atoms.size() == 1?
                new ReasonerAtomicQuery(atoms.iterator().next()) : new ReasonerQueryImpl(as, graph);
=======
    public static ReasonerQueryImpl create(Set<Atom> atoms, GraknTx graph){
        return atoms.size() == 1? new ReasonerAtomicQuery(atoms.iterator().next()) : new ReasonerQueryImpl(atoms, graph);
>>>>>>> af2276f104a6e1cf059ff5cdba6152b2211e523d
    }

    public static ReasonerAtomicQuery atomic(Conjunction<VarPatternAdmin> pattern, GraknTx graph){
        return new ReasonerAtomicQuery(pattern, graph);
    }

    public static ReasonerAtomicQuery atomic(Atom atom){
        return new ReasonerAtomicQuery(atom);
    }

    public static ReasonerAtomicQuery atomic(ReasonerQueryImpl q){
        return new ReasonerAtomicQuery(q);
    }
}
