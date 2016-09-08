/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.util.ErrorMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MindmapsFactoryBuilderTest {
    private final static String TEST_CONFIG = "../conf/test/mindmaps-tinker-test.properties";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test(expected=InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<MindmapsFactoryBuilder> c = MindmapsFactoryBuilder.class.getDeclaredConstructor();
        c.setAccessible(true);
        c.newInstance();
    }

    @Test
    public void testBuildMindmapsFactory(){
        MindmapsGraphFactory mgf = MindmapsFactoryBuilder.getFactory(TEST_CONFIG);
        assertThat(mgf, instanceOf(MindmapsTinkerGraphFactory.class));
    }

    @Test
    public void testSingleton(){
        MindmapsGraphFactory mgf1 = MindmapsFactoryBuilder.getFactory(TEST_CONFIG);
        MindmapsGraphFactory mgf2 = MindmapsFactoryBuilder.getFactory(TEST_CONFIG);
        assertEquals(mgf1, mgf2);
    }

    @Test
    public void testBadConfigPath(){
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage("rubbish"))
        ));
        MindmapsFactoryBuilder.getFactory("rubbish");
    }

}