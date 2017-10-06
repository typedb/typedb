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

package ai.grakn.engine.config;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.MockRedisRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertNotNull;

/**
 * Testing the {@link GraknEngineConfig} class
 *
 * @author alexandraorth
 */
public class GraknEngineConfigTest {

    private GraknEngineConfig configuration = GraknEngineConfig.create();

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(new SimpleURI(EngineTestHelper.config().getProperty(GraknEngineConfig.REDIS_HOST)).getPort());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGettingPropertyAndPropertyIsUndefinedInConfigurationFile_ExceptionIsThrown() {
        String property = "invalid";

        exception.expect(RuntimeException.class);
        exception.expectMessage(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(property, GraknEngineConfig.getConfigFilePath()));

        configuration.getProperty(property);
    }

    @Test
    public void whenGettingExistingProperty_PropertyIsReturned(){
        String property = "server.port";

        assertNotNull(configuration.getProperty(property));
    }
}
