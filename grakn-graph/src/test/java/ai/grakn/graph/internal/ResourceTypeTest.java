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

package ai.grakn.graph.internal;

import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ResourceTypeTest extends GraphTestBase{
    private ResourceType<String> resourceType;


    @Before
    public void buildGraph() {
        resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
    }

    @Test
    public void testDataType() throws Exception {
        assertEquals(ResourceType.DataType.STRING, resourceType.getDataType());
    }

    @Test
    public void testRegexValid(){
        assertNull(resourceType.getRegex());
        resourceType.setRegex("[abc]");
        assertEquals(resourceType.getRegex(), "[abc]");
    }

    @Test
    public void testRegexInvalid(){
        assertNull(resourceType.getRegex());
        expectedException.expect(PatternSyntaxException.class);
        resourceType.setRegex("[");
    }

    @Test
    public void testRegexSetOnNonString(){
        ResourceType<Long> thing = graknGraph.putResourceType("Random ID", ResourceType.DataType.LONG);
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(ErrorMessage.REGEX_NOT_STRING.getMessage(thing.getLabel()));
        thing.setRegex("blab");
    }

    @Test
    public void testRegexInstance(){
        resourceType.setRegex("[abc]");
        resourceType.putResource("a");
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(CoreMatchers.allOf(containsString("[abc]"), containsString("1"), containsString(resourceType.getLabel().getValue())));
        resourceType.putResource("1");
    }

    @Test
    public void testRegexInstanceChangeRegexWithInstances(){
        Resource<String> thing = resourceType.putResource("1");
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage("[abc]", thing.getId(), thing.getValue(), resourceType.getLabel()));
        resourceType.setRegex("[abc]");
    }

    @Test
    public void checkSuper() throws Exception{
        ResourceType<String> superConcept = graknGraph.putResourceType("super", ResourceType.DataType.STRING);
        ResourceType<String> resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        resourceType.superType(superConcept);
        assertThat(resourceType.superType(), instanceOf(ResourceType.class));
        assertEquals(superConcept, resourceType.superType());
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
}