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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.TxTestBase;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

public class AttributeTypeTest extends TxTestBase {
    private AttributeType<String> attributeType;


    @Before
    public void setup() {
        attributeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING);
    }

    @Test
    public void whenCreatingResourceTypeOfTypeString_DataTypeIsString() throws Exception {
        assertEquals(AttributeType.DataType.STRING, attributeType.getDataType());
    }

    @Test
    public void whenCreatingStringResourceTypeWithValidRegex_EnsureNoErrorsThrown(){
        assertNull(attributeType.getRegex());
        attributeType.setRegex("[abc]");
        assertEquals(attributeType.getRegex(), "[abc]");
    }

    @Test
    public void whenCreatingStringResourceTypeWithInvalidRegex_Throw(){
        assertNull(attributeType.getRegex());
        expectedException.expect(PatternSyntaxException.class);
        attributeType.setRegex("[");
    }

    @Test
    public void whenSettingRegexOnNonStringResourceType_Throw(){
        AttributeType<Long> thing = tx.putAttributeType("Random ID", AttributeType.DataType.LONG);
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.cannotSetRegex(thing).getMessage());
        thing.setRegex("blab");
    }

    @Test
    public void whenAddingResourceWhichDoesNotMatchRegex_Throw(){
        attributeType.setRegex("[abc]");
        attributeType.putAttribute("a");
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[abc]"), containsString("1"), containsString(attributeType.getLabel().getValue())));
        attributeType.putAttribute("1");
    }

    @Test
    public void whenSettingRegexOnResourceTypeWithResourceNotMatchingRegex_Throw(){
        attributeType.putAttribute("1");
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.regexFailure(attributeType, "1", "[abc]").getMessage());
        attributeType.setRegex("[abc]");
    }

    @Test
    public void whenGettingTheResourceFromAResourceType_ReturnTheResource(){
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING);

        Attribute c1 = t1.putAttribute("1");
        Attribute c2 = t2.putAttribute("2");

        assertEquals(c1, t1.getAttribute("1"));
        assertNull(t1.getAttribute("2"));

        assertEquals(c2, t2.getAttribute("2"));
        assertNull(t2.getAttribute("1"));
    }

    @Test
    public void whenCreatingMultipleResourceTypesWithDifferentRegexes_EnsureAllRegexesAreChecked(){
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING).setRegex("[b]");
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).setRegex("[abc]").sup(t1);

        //Valid Attribute
        Attribute<String> attribute = t2.putAttribute("b");

        //Invalid Attribute
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[b]"), containsString("b"), containsString(attribute.type().getLabel().getValue())));
        t2.putAttribute("a");
    }

    @Test
    public void whenSettingTheSuperTypeOfAStringResourceType_EnsureAllRegexesAreAppliedToResources(){
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING).setRegex("[b]");
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).setRegex("[abc]");

        //Future Invalid
        t2.putAttribute("a");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.regexFailure(t2, "a", "[b]").getMessage());
        t2.sup(t1);
    }

    @Test
    public void whenSettingRegexOfSuperType_EnsureAllRegexesAreApplied(){
        AttributeType<String> t1 = tx.putAttributeType("t1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("t2", AttributeType.DataType.STRING).setRegex("[abc]").sup(t1);
        t2.putAttribute("a");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.regexFailure(t1, "a", "[b]").getMessage());
        t1.setRegex("[b]");
    }

    @Test
    public void whenCreatingAResourceTypeOfTypeDate_EnsureTheTimeZoneIsSetTOADefaultAndDoesNotAffectRetreival() {

        // offset the time to GMT-8
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-8"));
        // get the local time (without timezone)
        LocalDateTime rightNow = LocalDateTime.now();
        // now add the timezone to the graph
        try (GraknSession session = Grakn.session(Grakn.IN_MEMORY, "somethingmorerandom")) {
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {
                AttributeType<LocalDateTime> aTime = graph.putAttributeType("aTime", AttributeType.DataType.DATE);
                aTime.putAttribute(rightNow);
                graph.commit();
            }
        }

        // offset the time to GMT where the colleague is working
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        // the colleague extracts the LocalTime which should be the same
        try (GraknSession session = Grakn.session(Grakn.IN_MEMORY, "somethingmorerandom")) {
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {
                AttributeType aTime = graph.getAttributeType("aTime");
                LocalDateTime databaseTime = (LocalDateTime) ((Attribute) aTime.instances().iterator().next()).getValue();

                // localTime should not have changed as it should not be sensitive to timezone
                assertEquals(rightNow, databaseTime);
            }
        }
    }
}