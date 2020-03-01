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
 */

package grakn.core.common.config.test;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.exception.ErrorMessage;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.InputStream;

import static junit.framework.TestCase.assertNotNull;

/**
 * Testing the Config class
 *
 */
public class ConfigTest {

    private final static InputStream TEST_CONFIG_FILE = ConfigTest.class.getClassLoader().getResourceAsStream("server/conf/grakn.properties");
    private final static Config configuration = Config.read(TEST_CONFIG_FILE);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @org.junit.Test
    public void whenGettingPropertyAndPropertyIsUndefinedInConfigurationFile_ExceptionIsThrown() {
        ConfigKey<String> key = ConfigKey.key("undefined");

        exception.expect(RuntimeException.class);
        exception.expectMessage(
                ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(key.name(), Config.CONFIG_FILE_PATH)
        );

        configuration.getProperty(key);
    }

    @org.junit.Test
    public void whenGettingExistingProperty_PropertyIsReturned(){
        assertNotNull(configuration.getProperty(ConfigKey.SERVER_HOST_NAME));
    }
}
