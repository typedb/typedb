/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package storage;

import ai.grakn.concept.Concept;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.IdProperty;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 *
 */
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

//        List<Answer> answerList;
//        Object x;
//        try (GraknTx tx = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("societal_model")).open(GraknTxType.WRITE)) {
////    answerList = tx.graql().match(Graql.var("x").isa("company").has("name")).get().execute();
////    answerList = tx.graql().match(Graql.var("x").isa("company").has("name", Graql.var("y"))).get().execute();
////    answerList = tx.graql().match(Graql.var("x").isa("company").has("name","Ganesh")).get().execute();
////    answerList = tx.graql().match(Graql.var("x").isa("company"), Graql.var("x").has("name","Ganesh")).get().execute();
////    answerList = tx.graql().match(Graql.var("x").isa("company")).get().execute();
////    x = answerList.get(0).get("y").asAttribute().getValue();
//            answerList = tx.graql().insert(Graql.var("x").isa("company").has("name", Graql.var("y")), Graql.var("y").val("James")).execute();
//            tx.commit();
//        }
//        insert $x has name "Tomas", has name $y; $y "Tomas2"; $x id V28336136;

        HashSet<Var> insertVarsWithoutIds = getVarsWithoutIds(insertVarPatternsIterator);
        //Close to getting attributes query.admin().varPatterns().iterator().next().admin().getProperty(HasAttributeTypeProperty.class)
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

        //TODO I don;t think this works at present.
        HashSet<Var> varsWithoutIds = new HashSet<>();
        HashSet<Var> varsWithIds = new HashSet<>();

        while (varPatternAdminIterator.hasNext()) {

            VarPatternAdmin varPatternAdmin = varPatternAdminIterator.next();
            Optional<IdProperty> idProperty = varPatternAdmin.getProperty(IdProperty.class);
            if(idProperty.isPresent()) {
                varsWithIds.add(varPatternAdmin.var());
            } else {
                //ConceptId id = idProperty.get().id();  // How to get the id, but in fact we don't care

                // If no id is present, then add to the set
                varsWithoutIds.add(varPatternAdmin.var());
            }
        }
        return varsWithoutIds;

    }
}
