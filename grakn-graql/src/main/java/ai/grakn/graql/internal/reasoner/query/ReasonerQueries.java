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
