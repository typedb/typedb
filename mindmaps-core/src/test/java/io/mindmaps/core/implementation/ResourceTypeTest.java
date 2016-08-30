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

package io.mindmaps.core.implementation;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.Data;
import io.mindmaps.core.implementation.exception.InvalidConceptValueException;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.regex.PatternSyntaxException;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ResourceTypeTest {

    private MindmapsTransactionImpl mindmapsGraph;
    private ResourceType<String> resourceType;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().getTransaction();
        mindmapsGraph.initialiseMetaConcepts();
        resourceType = mindmapsGraph.putResourceType("Resource Type", Data.STRING);
    }

    @Test
    public void testDataType() throws Exception {
        assertEquals(Data.STRING, resourceType.getDataType());
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
        ResourceType<Long> thing = mindmapsGraph.putResourceType("Random ID", Data.LONG);
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.REGEX_NOT_STRING.getMessage(thing.toString()))
        ));
        thing.setRegex("blab");
    }

    @Test
    public void testRegexInstance(){
        resourceType.setRegex("[abc]");
        mindmapsGraph.putResource("a", resourceType);
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(allOf(
                containsString("regular expressions")
        ));
        mindmapsGraph.putResource("1", resourceType);
    }

    @Test
    public void testRegexInstanceChangeRegexWithInstances(){
        Resource<String> thing = mindmapsGraph.putResource("1", resourceType);
        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage("[abc]", thing.toString()))
        ));
        resourceType.setRegex("[abc]");
    }

    @Test
    public void checkSuper() throws Exception{
        ResourceType superConcept = mindmapsGraph.putResourceType("super", Data.STRING);
        ResourceType resourceType = mindmapsGraph.putResourceType("resourceType", Data.STRING);
        resourceType.superType(superConcept);
        assertThat(resourceType.superType(), instanceOf(ResourceType.class));
        assertEquals(superConcept, resourceType.superType());
    }
}