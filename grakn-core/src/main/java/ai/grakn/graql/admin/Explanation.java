package ai.grakn.graql.admin;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * <p>
 * Base class for explanation classes.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Explanation {

    private final Set<Answer> answers;

    public Explanation(){ answers = new HashSet<>();}
    public Explanation(Set<Answer> ans){ answers = ans;}
    public Explanation(Explanation e){ answers = new HashSet<>(e.answers);}

    public Explanation copy(){ return new Explanation(this);};

    public boolean addAnswer(Answer a){ return answers.add(a);}
    public Set<Answer> getAnswers(){ return answers;}

    public boolean isRuleExplanation(){ return false;};
    public boolean isLookupExplanation(){ return false;};

    public ReasonerQuery getQuery(){ return null;}
}
