/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.Grakn;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceTypeTest {

    private AbstractGraknGraph graknGraph;
    private ResourceType<String> resourceType;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() {
        graknGraph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.initialiseMetaConcepts();
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
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.REGEX_NOT_STRING.getMessage(thing.toString()))
        ));
        thing.setRegex("blab");
    }

    @Test
    public void testRegexInstance(){
        resourceType.setRegex("[abc]");
        graknGraph.putResource("a", resourceType);
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(allOf(
                containsString("regular expressions")
        ));
        graknGraph.putResource("1", resourceType);
    }

    @Test
    public void testRegexInstanceChangeRegexWithInstances(){
        Resource<String> thing = graknGraph.putResource("1", resourceType);
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage("[abc]", thing.toString()))
        ));
        resourceType.setRegex("[abc]");
    }

    @Test
    public void testGetUniqueResourceType(){
        ResourceType unique = graknGraph.putResourceTypeUnique("Random ID", ResourceType.DataType.LONG);
        ResourceType notUnique = graknGraph.putResourceType("Random ID 2", ResourceType.DataType.LONG);

        assertTrue(unique.isUnique());
        assertFalse(notUnique.isUnique());
    }

    @Test
    public void checkSuper() throws Exception{
        ResourceType superConcept = graknGraph.putResourceType("super", ResourceType.DataType.STRING);
        ResourceType resourceType = graknGraph.putResourceType("resourceType", ResourceType.DataType.STRING);
        resourceType.superType(superConcept);
        assertThat(resourceType.superType(), instanceOf(ResourceType.class));
        assertEquals(superConcept, resourceType.superType());
    }
}