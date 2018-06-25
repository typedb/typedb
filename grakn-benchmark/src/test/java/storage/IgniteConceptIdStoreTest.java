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
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteConceptIdStoreTest {

    private IgniteConceptIdStore store;
    private HashSet<String> typeLabelsSet;
    private ArrayList<String> typeLabels;
    private ArrayList<String> conceptIds;
    private ArrayList<Concept> conceptMocks;
    private String typeLabel;

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {

        typeLabel = "person";
//        typeLabels = new ArrayList<>();
//        typeLabels.add(typeLabel);
//        typeLabels.add("company");
//        typeLabels.add("dinosaur");
        typeLabelsSet = new HashSet<>();
        typeLabelsSet.add(typeLabel);

        conceptIds = new ArrayList<>();
        conceptIds.add("V123456");
        conceptIds.add("V298345");
        conceptIds.add("V380325");
        conceptIds.add("V4");
        conceptIds.add("V5");
        conceptIds.add("V6");
        conceptIds.add("V7");

        conceptMocks = new ArrayList<>();

        Iterator<String> idIterator = conceptIds.iterator();
//        Iterator<String> labelIterator = typeLabels.iterator();

        while (idIterator.hasNext()) {

            // Concept
            Concept conceptMock = mock(Concept.class);
            this.conceptMocks.add(conceptMock);

            // Thing
            Thing thingMock = mock(Thing.class);
            when(conceptMock.asThing()).thenReturn(thingMock);

            // ConceptID
            ConceptId conceptId = ConceptId.of(idIterator.next());
            when(thingMock.getId()).thenReturn(conceptId);

            // Concept Type
            Type conceptTypeMock = mock(Type.class);
            when(thingMock.type()).thenReturn(conceptTypeMock);

            // Concept Type getLabel()
            Label label = Label.of(typeLabel);
            when(conceptTypeMock.getLabel()).thenReturn(label);
        }
    }

    @Test
    public void whenConceptIdsAreAdded_conceptIdsAreInTheDB() throws SQLException, ClassNotFoundException {
        IgniteConceptIdStore.clean(this.typeLabelsSet);
        this.store = new IgniteConceptIdStore(this.typeLabelsSet);

        // Add all of the elements
        for (Concept conceptMock : this.conceptMocks) {
            this.store.add(conceptMock);
        }

        int counter = 0;
        // Check objects were added to the db
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + this.typeLabel)) {
                while (rs.next()) {
                    counter++;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        assertEquals(7, counter);
    }

    @Test
    public void whenConceptIsAdded_conceptIdCanBeRetrieved() throws SQLException, ClassNotFoundException {
        IgniteConceptIdStore.clean(this.typeLabelsSet);
        this.store = new IgniteConceptIdStore(typeLabelsSet);

        int index = 0;
        this.store.add(this.conceptMocks.get(index));
        String personId = this.store.get(this.typeLabel, index);
        System.out.println("Found id: " + personId);
        assertEquals(personId, this.conceptIds.get(index));
    }

    @Test
    public void whenGettingIdWithOffset_correctIdIsReturned() throws SQLException, ClassNotFoundException {
        IgniteConceptIdStore.clean(this.typeLabelsSet);
        this.store = new IgniteConceptIdStore(this.typeLabelsSet);

        int index = 4;
        // Add all of the elements

        for (Concept conceptMock : this.conceptMocks) {
            this.store.add(conceptMock);
        }

        String personId = this.store.get(this.typeLabel, index);
        System.out.println("Found id: " + personId);
        assertEquals(this.conceptIds.get(index), personId);
    }

    @Test
    public void whenCountingTypeInstances_resultIsCorrect() throws SQLException, ClassNotFoundException {
        IgniteConceptIdStore.clean(this.typeLabelsSet);
        this.store = new IgniteConceptIdStore(this.typeLabelsSet);

        for (Concept conceptMock : this.conceptMocks) {
            this.store.add(conceptMock);
        }

        int count = this.store.typeCount(this.typeLabel);
        assertEquals(7, count);
    }
}