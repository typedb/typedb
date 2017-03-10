package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.admin.ReasonerQuery;

/**
 *
 * <p>
 * Explanation class for db lookup.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class LookupExplanation extends Explanation {

    private final ReasonerQuery query;

    public LookupExplanation(ReasonerQuery q){ this.query = q;}
    public LookupExplanation(LookupExplanation exp){
        super(exp);
        this.query = exp.getQuery().copy();
    }

    @Override
    public Explanation copy(){ return new LookupExplanation(this);}

    @Override
    public ReasonerQuery getQuery(){ return query;}

    @Override
    public boolean isLookupExplanation(){ return true;}
}
