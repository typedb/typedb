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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class InsertionAnalysisTest {

    private ArrayList<ConceptMap> mockConceptMaps(Map<Var, String> variables) {

        ArrayList<ConceptMap> answerList = new ArrayList<>();
        ConceptMap answerMock = mock(ConceptMap.class);
        for (Map.Entry<Var, String> variable : variables.entrySet()) {

            // Mock the answer object
            Concept conceptMock = mock(Concept.class);
            Thing thingMock = mock(Thing.class);
            when(conceptMock.asThing()).thenReturn(thingMock);
            when(thingMock.id()).thenReturn(ConceptId.of(variable.getValue()));
            when(answerMock.get(variable.getKey())).thenReturn(conceptMock);

        }
        answerList.add(answerMock);
        return answerList;
    }

    @Test
    public void whenEntityInserted_IdentifyEntityWasInserted() {

        Var x = Graql.var("x");
        InsertQuery query = Graql.insert(x.isa("company"));

        HashMap<Var, String> vars = new HashMap<>();
        vars.put(x, "V123456");
        ArrayList<ConceptMap> answerList = this.mockConceptMaps(vars);

        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(query, answerList);

        assertEquals(1, insertedConcepts.size());
        assertEquals("V123456", insertedConcepts.iterator().next().asThing().id().toString());
    }

    @Test
    public void whenRelationshipInserted_IdentifyRelationshipWasInserted() {

        String rId = "Vr";
        String xId = "Vx";
        String yId = "Vy";
        String zId = "Vz";

        Var r = Graql.var("r");
        Var x = Graql.var("x").asUserDefined();
        Var y = Graql.var("y").asUserDefined();
        Var z = Graql.var("z").asUserDefined();

        HashMap<Var, String> vars = new HashMap<>();
        vars.put(r, rId);
        vars.put(x, xId);
        vars.put(y, yId);
        vars.put(z, zId);

        InsertQuery query = Graql.match(
                x.id(ConceptId.of(xId))
                        .and(y.id(ConceptId.of(xId)))
                        .and(z.id(ConceptId.of(xId)))
        ).insert(
                r.isa("employment")
                        .rel("employee", x)
                        .rel("employee", y)
                        .rel("employee", z));

        ArrayList<ConceptMap> answerList = this.mockConceptMaps(vars);

        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(query, answerList);

        assertEquals(1, insertedConcepts.size());
        assertEquals(rId, insertedConcepts.iterator().next().asThing().id().toString());
    }

    @Test
    public void whenRelationshipInsertedWithIdsInInsert_IdentifyRelationshipWasInserted() {

        String rId = "Vr";
        String xId = "Vx";
        String yId = "Vy";
        String zId = "Vz";

        Var r = Graql.var("r");
        Var x = Graql.var("x").asUserDefined();
        Var y = Graql.var("y").asUserDefined();
        Var z = Graql.var("z").asUserDefined();

        HashMap<Var, String> vars = new HashMap<>();
        vars.put(r, rId);
        vars.put(x, xId);
        vars.put(y, yId);
        vars.put(z, zId);

        InsertQuery query = Graql.insert(
                r.isa("employment")
                        .rel("employee", x)
                        .rel("employee", y)
                        .rel("employee", z),
                        x.id(ConceptId.of(xId)),
                        y.id(ConceptId.of(xId)),
                        z.id(ConceptId.of(xId)));

        ArrayList<ConceptMap> answerList = this.mockConceptMaps(vars);

        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(query, answerList);

        assertEquals(1, insertedConcepts.size());
        assertEquals(rId, insertedConcepts.iterator().next().asThing().id().toString());
    }

    @Test
    public void whenAttributeInserted_IdentifyAttributeWasInserted() {
//        varPatternAdmin.commonVars()

        String xId = "Vx";
        String yId = "Vy";

        String cAttr = "c-name";

        Var x = Graql.var("x").asUserDefined();
        Var y = Graql.var("y").asUserDefined();

        HashMap<Var, String> vars = new HashMap<>();
        vars.put(x, xId);
        vars.put(y, yId);

//        InsertQuery query = Graql.insert(x.isa("company").has("name", cAttr));
        InsertQuery query = Graql.insert(x.isa("company").has("name", y), x.id(ConceptId.of(xId)), y.val(cAttr));

        ArrayList<ConceptMap> answerList = this.mockConceptMaps(vars);

        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(query, answerList);

        assertEquals(1, insertedConcepts.size());
        assertEquals(yId, insertedConcepts.iterator().next().asThing().id().toString());
    }

    @Test
    public void whenAttributeInsertedWithIdInClause_IdentifyAttributeWasInserted() {
//        varPatternAdmin.commonVars()

        String xId = "Vx";
        String yId = "Vy";

        String cAttr = "c-name";

        Var x = Graql.var("x").asUserDefined();
        Var y = Graql.var("y").asUserDefined();

        HashMap<Var, String> vars = new HashMap<>();
        vars.put(x, xId);
        vars.put(y, yId);

//        InsertQuery query = Graql.insert(x.isa("company").has("name", cAttr));
        InsertQuery query = Graql.insert(x.isa("company").has("name", y).id(ConceptId.of(xId)), y.val(cAttr));

        ArrayList<ConceptMap> answerList = this.mockConceptMaps(vars);

        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(query, answerList);

        assertEquals(1, insertedConcepts.size());
        assertEquals(yId, insertedConcepts.iterator().next().asThing().id().toString());
    }
}