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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.config;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import ai.grakn.util.ErrorMessage;
import ai.grakn.test.rule.InMemoryRedisContext;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.Iterables;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertNotNull;

/**
 * Testing the {@link GraknConfig} class
 *
 * @author alexandraorth
 */
public class GraknConfigTest {

    private static GraknConfig configuration = GraknConfig.create();

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create(new SimpleURI(Iterables.getOnlyElement(configuration.getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGettingPropertyAndPropertyIsUndefinedInConfigurationFile_ExceptionIsThrown() {
        GraknConfigKey<String> key = GraknConfigKey.key("undefined");

        exception.expect(RuntimeException.class);
        exception.expectMessage(
                ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(key.name(), GraknConfig.CONFIG_FILE_PATH)
        );

        configuration.getProperty(key);
    }

    @Test
    public void whenGettingExistingProperty_PropertyIsReturned(){
        assertNotNull(configuration.getProperty(GraknConfigKey.SERVER_PORT));
    }
}
