package storage;

import ai.grakn.concept.Concept;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IdProperty;

import java.util.*;

public class InsertionAnalysis {

    public static HashSet<Concept> getInsertedConcepts(InsertQuery query, List<Answer> answers) {
        /*
        Method

        Get the set of variables used in the insert

        Find those in the insert without an id

        If there's a match statement
            Get the set of variables used in the match

            Find those variables without an id

            Remove any varibales in the insert that also exist in the match

        Those variables remaining must have been inserted
        Then find those variables in the answer, and get their concepts (there should be only one concept per variable?)

         */

        Iterator<VarPatternAdmin> insertVarPatternsIterator = query.admin().varPatterns().iterator();

        HashSet<Var> insertVarsWithoutIds = getVarsWithoutIds(insertVarPatternsIterator);

        Match match = query.admin().match();
        if (match != null) {
            // We only do anything with the match clause if it exists
            Iterator<VarPatternAdmin> matchVarPatternsIterator = match.admin().getPattern().varPatterns().iterator();
            HashSet<Var> matchVarsWithoutIds = getVarsWithoutIds(matchVarPatternsIterator);
            insertVarsWithoutIds.removeAll(matchVarsWithoutIds);
        }

        HashSet<Concept> resultConcepts = new HashSet<>();

        for (Answer answer : answers){
            for (Var insertVarWithoutId : insertVarsWithoutIds) {
                resultConcepts.add(answer.get(insertVarWithoutId));
            }
        }

        return resultConcepts;
    }

    private static HashSet<Var> getVarsWithoutIds(Iterator<VarPatternAdmin> varPatternAdminIterator) {

        HashSet<Var> varsWithoutIds = new HashSet<>();

        while (varPatternAdminIterator.hasNext()) {

            VarPatternAdmin varPatternAdmin = varPatternAdminIterator.next();
            Optional<IdProperty> idProperty = varPatternAdmin.getProperty(IdProperty.class);
            if(!idProperty.isPresent()) {
                //ConceptId id = idProperty.get().id();  // How to get the id, but in fact we don't care

                // If no id is present, then add to the set
                varsWithoutIds.add(varPatternAdmin.var());
            }
        }
        return varsWithoutIds;

    }
}
