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

package grakn.core.kb.internal.concept;

import grakn.core.Session;
import grakn.core.Transaction;
import grakn.core.concept.Attribute;
import grakn.core.concept.AttributeType;
import grakn.core.exception.TransactionException;
import grakn.core.test.rule.ConcurrentGraknServer;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

public class AttributeTypeIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();
    private AttributeType<String> attributeType;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();


    private Transaction tx;
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
        attributeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING);
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    @Test
    public void whenCreatingResourceTypeOfTypeString_DataTypeIsString() throws Exception {
        assertEquals(AttributeType.DataType.STRING, attributeType.dataType());
    }

    @Test
    public void whenCreatingStringResourceTypeWithValidRegex_EnsureNoErrorsThrown() {
        assertNull(attributeType.regex());
        attributeType.regex("[abc]");
        assertEquals(attributeType.regex(), "[abc]");
    }

    @Test
    public void whenCreatingStringResourceTypeWithInvalidRegex_Throw() {
        assertNull(attributeType.regex());
        expectedException.expect(PatternSyntaxException.class);
        attributeType.regex("[");
    }

    @Test
    public void whenSettingRegexOnNonStringResourceType_Throw() {
        AttributeType<Long> thing = tx.putAttributeType("Random ID", AttributeType.DataType.LONG);
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.cannotSetRegex(thing).getMessage());
        thing.regex("blab");
    }

    @Test
    public void whenAddingResourceWhichDoesNotMatchRegex_Throw() {
        attributeType.regex("[abc]");
        attributeType.create("a");
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[abc]"), containsString("1"), containsString(attributeType.label().getValue())));
        attributeType.create("1");
    }

    @Test
    public void whenSettingRegexOnResourceTypeWithResourceNotMatchingRegex_Throw() {
        attributeType.create("1");
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.regexFailure(attributeType, "1", "[abc]").getMessage());
        attributeType.regex("[abc]");
    }

    @Test
    public void whenGettingTheResourceFromAResourceType_ReturnTheResource() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING);

        Attribute c1 = t1.create("1");
        Attribute c2 = t2.create("2");

        assertEquals(c1, t1.attribute("1"));
        assertNull(t1.attribute("2"));

        assertEquals(c2, t2.attribute("2"));
        assertNull(t2.attribute("1"));
    }

    @Test
    public void whenCreatingMultipleResourceTypesWithDifferentRegexes_EnsureAllRegexesAreChecked() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING).regex("[b]");
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).regex("[abc]").sup(t1);

        //Valid Attribute
        Attribute<String> attribute = t2.create("b");

        //Invalid Attribute
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[b]"), containsString("b"), containsString(attribute.type().label().getValue())));
        t2.create("a");
    }

    @Test
    public void whenSettingTheSuperTypeOfAStringResourceType_EnsureAllRegexesAreAppliedToResources() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING).regex("[b]");
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).regex("[abc]");

        //Future Invalid
        t2.create("a");

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.regexFailure(t2, "a", "[b]").getMessage());
        t2.sup(t1);
    }

    @Test
    public void whenSettingRegexOfSuperType_EnsureAllRegexesAreApplied() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).regex("[abc]").sup(t1);
        t2.create("a");

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.regexFailure(t1, "a", "[b]").getMessage());
        t1.regex("[b]");
    }

    @Test
    public void whenCreatingAResourceTypeOfTypeDate_EnsureTheTimeZoneIsSetTOADefaultAndDoesNotAffectRetrieval() {

        // offset the time to GMT-8
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-8"));
        // get the local time (without timezone)
        LocalDateTime rightNow = LocalDateTime.now();
        // now add the timezone to the graph
        try (Session session = server.sessionWithNewKeyspace()) {
            try (Transaction graph = session.transaction(Transaction.Type.WRITE)) {
                AttributeType<LocalDateTime> aTime = graph.putAttributeType("aTime", AttributeType.DataType.DATE);
                aTime.create(rightNow);
                graph.commit();
            }
            // offset the time to GMT where the colleague is working
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            // the colleague extracts the LocalTime which should be the same
            try (Transaction graph = session.transaction(Transaction.Type.WRITE)) {
                AttributeType aTime = graph.getAttributeType("aTime");
                LocalDateTime databaseTime = (LocalDateTime) ((Attribute) aTime.instances().iterator().next()).value();

                // localTime should not have changed as it should not be sensitive to timezone
                assertEquals(rightNow, databaseTime);
            }
        }
    }
}