/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


package grakn.core.server.kb.cache;

import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class ConceptCachesIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }

    @Test
    public void whenCreatingResource_EnsureTheResourcesDataTypeIsTheSameAsItsType() throws Exception {
        AttributeType<String> attributeType = tx.putAttributeType("attributeType", AttributeType.DataType.STRING);
        Attribute attribute = attributeType.create("resource");
        assertEquals(AttributeType.DataType.STRING, attribute.dataType());
    }
}
