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

package ai.grakn.engine.util;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static junit.framework.TestCase.assertTrue;

public class ConfigPropertiesTest {

    @Before
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field instance = ConfigProperties.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testConfRelativePath(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,"../conf/test/tinker/grakn-engine.properties");
        assertTrue(ConfigProperties.getInstance().getConfigFilePath().equals(System.getProperty("user.dir")+"/../conf/test/tinker/grakn-engine.properties"));
    }

    @Test
    public void testConfAbsolutePath(){
        String projectDir = System.getProperty("user.dir").substring(0,System.getProperty("user.dir").length()-"grakn-engine".length());
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,projectDir+"conf/test/tinker/grakn-engine.properties");
        assertTrue(ConfigProperties.getInstance().getConfigFilePath().equals(projectDir+"conf/test/tinker/grakn-engine.properties"));
    }
}
