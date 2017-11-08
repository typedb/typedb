package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.reasoner.state.QueryStateBase;
import java.util.Iterator;

/**
 * Created by kasper on 08/11/17.
 */
public class QueryStateIterator  {

    private final Iterator<Answer> dbIterator;
    private final MultiUnifier cacheUnifier;

    private final Iterator<QueryStateBase> subGoalIterator;

    QueryStateIterator(Iterator<Answer> dbIterator, MultiUnifier cacheUnifier, Iterator<QueryStateBase> subGoalIterator){
        this.dbIterator = dbIterator;
        this.cacheUnifier = cacheUnifier;
        this.subGoalIterator = subGoalIterator;
    }

    public Iterator<Answer> dbIterator(){ return dbIterator;}
    public MultiUnifier cacheUnifier(){ return cacheUnifier;}
    public Iterator<QueryStateBase> subGoalIterator(){ return subGoalIterator;}
}
