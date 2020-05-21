package grakn.core.graql.reasoner.explanation;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;

import java.util.Collections;

public class CompositeExplanation extends Explanation {

    public CompositeExplanation(ConceptMap ans) {
        super(Collections.singletonList(ans));
    }
}
