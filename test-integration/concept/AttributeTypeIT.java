/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept;

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
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
    public static final GraknTestServer server = new GraknTestServer();
    private AttributeType<String> attributeType;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();


    private Transaction tx;
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
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
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.cannotSetRegex(thing).getMessage());
        thing.regex("blab");
    }

    @Test
    public void whenAddingResourceWhichDoesNotMatchRegex_Throw() {
        attributeType.regex("[abc]");
        attributeType.create("a");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[abc]"), containsString("1"), containsString(attributeType.label().getValue())));
        attributeType.create("1");
    }

    @Test
    public void whenSettingRegexOnResourceTypeWithResourceNotMatchingRegex_Throw() {
        attributeType.create("1");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.regexFailure(attributeType, "1", "[abc]").getMessage());
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
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[b]"), containsString("b"), containsString(attribute.type().label().getValue())));
        t2.create("a");
    }

    @Test
    public void whenSettingTheSuperTypeOfAStringResourceType_EnsureAllRegexesAreAppliedToResources() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING).regex("[b]");
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).regex("[abc]");

        //Future Invalid
        t2.create("a");

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.regexFailure(t2, "a", "[b]").getMessage());
        t2.sup(t1);
    }

    @Test
    public void whenSettingRegexOfSuperType_EnsureAllRegexesAreApplied() {
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).regex("[abc]").sup(t1);
        t2.create("a");

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.regexFailure(t1, "a", "[b]").getMessage());
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
            try (Transaction graph = session.writeTransaction()) {
                AttributeType<LocalDateTime> aTime = graph.putAttributeType("aTime", AttributeType.DataType.DATE);
                aTime.create(rightNow);
                graph.commit();
            }
            // offset the time to GMT where the colleague is working
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            // the colleague extracts the LocalTime which should be the same
            try (Transaction graph = session.writeTransaction()) {
                AttributeType aTime = graph.getAttributeType("aTime");
                LocalDateTime databaseTime = (LocalDateTime) ((Attribute) aTime.instances().iterator().next()).value();

                // localTime should not have changed as it should not be sensitive to timezone
                assertEquals(rightNow, databaseTime);
            }
        }
    }


    @Test
    public void whenNamingAttributeTypeValue_NoErrorsThrown() {
        AttributeType<String> valueAttributeType = tx.putAttributeType("value", AttributeType.DataType.STRING);

        Attribute<String> attribute = valueAttributeType.create("testing");
        EntityType person = tx.putEntityType("person").has(valueAttributeType);
        person.create().has(attribute);
        tx.commit();

        tx = session.readTransaction();
        tx.execute(Graql.parse("match $x isa @has-attribute; get;").asGet());
        tx.execute(Graql.parse("match $x isa @has-value; get;").asGet());
        tx.execute(Graql.parse("match (@has-value-value: $attr, @has-value-owner: $person) isa @has-value; get;").asGet());
    }

    @Test
    public void whenNamingAttributeTypeOwner_NoErrorsThrown() {
        AttributeType<String> ownerAttributeType = tx.putAttributeType("owner", AttributeType.DataType.STRING);

        Attribute<String> attribute = ownerAttributeType.create("testing");
        EntityType person = tx.putEntityType("person").has(ownerAttributeType);
        person.create().has(attribute);
        tx.commit();

        tx = session.readTransaction();
        tx.execute(Graql.parse("match $x isa @has-attribute; get;").asGet());
        tx.execute(Graql.parse("match $x isa @has-owner; get;").asGet());
        tx.execute(Graql.parse("match (@has-owner-value: $attr, @has-owner-owner: $person) isa @has-owner; get;").asGet());
    }
}