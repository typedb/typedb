/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graph.internal.concept;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.GraphTestBase;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

public class ResourceTypeTest extends GraphTestBase {
    private ResourceType<String> resourceType;


    @Before
    public void buildGraph() {
        resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
    }

    @Test
    public void whenCreatingResourceTypeOfTypeString_DataTypeIsString() throws Exception {
        assertEquals(ResourceType.DataType.STRING, resourceType.getDataType());
    }

    @Test
    public void whenCreatingStringResourceTypeWithValidRegex_EnsureNoErrorsThrown(){
        assertNull(resourceType.getRegex());
        resourceType.setRegex("[abc]");
        assertEquals(resourceType.getRegex(), "[abc]");
    }

    @Test
    public void whenCreatingStringResourceTypeWithInvalidRegex_Throw(){
        assertNull(resourceType.getRegex());
        expectedException.expect(PatternSyntaxException.class);
        resourceType.setRegex("[");
    }

    @Test
    public void whenSettingRegexOnNonStringResourceType_Throw(){
        ResourceType<Long> thing = graknGraph.putResourceType("Random ID", ResourceType.DataType.LONG);
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.cannotSetRegex(thing).getMessage());
        thing.setRegex("blab");
    }

    @Test
    public void whenAddingResourceWhichDoesNotMatchRegex_Throw(){
        resourceType.setRegex("[abc]");
        resourceType.putResource("a");
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[abc]"), containsString("1"), containsString(resourceType.getLabel().getValue())));
        resourceType.putResource("1");
    }

    @Test
    public void whenSettingRegexOnResourceTypeWithResourceNotMatchingRegex_Throw(){
        resourceType.putResource("1");
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.regexFailure(resourceType, "1", "[abc]").getMessage());
        resourceType.setRegex("[abc]");
    }

    @Test
    public void whenGettingTheResourceFromAResourceType_ReturnTheResource(){
        ResourceType<String> t1 = graknGraph.putResourceType("t1", ResourceType.DataType.STRING);
        ResourceType<String> t2 = graknGraph.putResourceType("t2", ResourceType.DataType.STRING);

        Resource c1 = t1.putResource("1");
        Resource c2 = t2.putResource("2");

        assertEquals(c1, t1.getResource("1"));
        assertNull(t1.getResource("2"));

        assertEquals(c2, t2.getResource("2"));
        assertNull(t2.getResource("1"));
    }

    @Test
    public void whenCreatingMultipleResourceTypesWithDifferentRegexes_EnsureAllRegexesAreChecked(){
        ResourceType<String> t1 = graknGraph.putResourceType("t1", ResourceType.DataType.STRING).setRegex("[b]");
        ResourceType<String> t2 = graknGraph.putResourceType("t2", ResourceType.DataType.STRING).setRegex("[abc]").sup(t1);

        //Valid Resource
        Resource<String> resource = t2.putResource("b");

        //Invalid Resource
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[b]"), containsString("b"), containsString(resource.type().getLabel().getValue())));
        t2.putResource("a");
    }

    @Test
    public void whenSettingTheSuperTypeOfAStringResourceType_EnsureAllRegexesAreAppliedToResources(){
        ResourceType<String> t1 = graknGraph.putResourceType("t1", ResourceType.DataType.STRING).setRegex("[b]");
        ResourceType<String> t2 = graknGraph.putResourceType("t2", ResourceType.DataType.STRING).setRegex("[abc]");

        //Future Invalid
        Resource<String> resource = t2.putResource("a");

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.regexFailure(t2, "a", "[b]").getMessage());
        t2.sup(t1);
    }

    @Test
    public void whenSettingRegexOfSuperType_EnsureAllRegexesAreApplied(){
        ResourceType<String> t1 = graknGraph.putResourceType("t1", ResourceType.DataType.STRING);
        ResourceType<String> t2 = graknGraph.putResourceType("t2", ResourceType.DataType.STRING).setRegex("[abc]").sup(t1);
        Resource<String> resource = t2.putResource("a");

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.regexFailure(t1, "a", "[b]").getMessage());
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
                ResourceType<LocalDateTime> aTime = graph.putResourceType("aTime", ResourceType.DataType.DATE);
                aTime.putResource(rightNow);
                graph.commit();
            }
        }

        // offset the time to GMT where the colleague is working
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        // the colleague extracts the LocalTime which should be the same
        try (GraknSession session = Grakn.session(Grakn.IN_MEMORY, "somethingmorerandom")) {
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
                ResourceType aTime = graph.getResourceType("aTime");
                LocalDateTime databaseTime = (LocalDateTime) ((Resource) aTime.instances().iterator().next()).getValue();

                // localTime should not have changed as it should not be sensitive to timezone
                assertEquals(rightNow, databaseTime);
            }
        }
    }
}