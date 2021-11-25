/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.server.test;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.CommandLine;
import com.vaticle.typedb.core.server.common.Configuration;
import com.vaticle.typedb.core.server.common.ConfigKVParser;
import com.vaticle.typedb.core.server.common.Util;
import org.junit.Test;

import java.nio.file.Path;
import java.util.HashSet;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_UNEXPECTED_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONFIG_OPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CONFIGURATION_OPTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigurationTest {

    @Test
    public void default_file_is_read() {
        Configuration configuration = (new Configuration.Parser()).getDefault();
        assertTrue(configuration.dataDir().toString().endsWith("server/data"));
        assertEquals(1729, configuration.port());
        assertFalse(configuration.vaticleFactory().trace());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("dir"));
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().path().toString().endsWith("server/logs"));
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFileMB() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesCount() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesGB() > 0);
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", configuration.log().logger().defaultLogger().level());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }

    @Test
    public void minimal_config_with_absolute_paths_is_read() {
        Path config_minimal_abs_paths = Util.getTypedbDir().resolve("server/test/config_minimal_abs_path.yml");
        Configuration configuration = (new Configuration.Parser()).parse(config_minimal_abs_paths, new HashSet<>());
        assertTrue(configuration.dataDir().isAbsolute());
        assertEquals(1730, configuration.port());
        assertFalse(configuration.vaticleFactory().trace());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("dir"));
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().path().isAbsolute());
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFileMB() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesCount() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesGB() > 0);
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", configuration.log().logger().defaultLogger().level());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }

    @Test
    public void config_invalid_path_throws() {
        Path config_missing = Util.getTypedbDir().resolve("server/test/missing.yml");
        try {
            (new Configuration.Parser()).parse(config_missing, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_FILE_NOT_FOUND.code(), e.code().get());
        }
    }

    @Test
    public void config_file_missing_port_throws() {
        Path config_missing_log = Util.getTypedbDir().resolve("server/test/config_missing_port.yml");
        try {
            (new Configuration.Parser()).parse(config_missing_log, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(MISSING_CONFIG_OPTION.code(), e.code().get());
            assertEquals(MISSING_CONFIG_OPTION.message("port"), e.getMessage());
        }
    }

    @Test
    public void config_file_missing_debugger_throws() {
        Path config_missing_log_debugger = Util.getTypedbDir().resolve("server/test/config_missing_debugger.yml");
        try {
            (new Configuration.Parser()).parse(config_missing_log_debugger, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(MISSING_CONFIG_OPTION.code(), e.code().get());
            assertEquals(MISSING_CONFIG_OPTION.message("log.debugger"), e.getMessage());
        }
    }

    @Test
    public void config_file_invalid_output_reference_throws() {
        Path config_invalid_output = Util.getTypedbDir().resolve("server/test/config_invalid_logger_output.yml");
        try {
            (new Configuration.Parser()).parse(config_invalid_output, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_OUTPUT_UNRECOGNISED.code(), e.code().get());
        }
    }

    @Test
    public void config_file_wrong_path_type_throws() {
        Path config_invalid_path_Type = Util.getTypedbDir().resolve("server/test/config_wrong_path_type.yml");
        try {
            (new Configuration.Parser()).parse(config_invalid_path_Type, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_UNEXPECTED_VALUE_TYPE.code(), e.code().get());
            assertEquals(CONFIG_UNEXPECTED_VALUE_TYPE.message("data", "1729[Integer]", ConfigKVParser.ValueParser.Leaf.STRING.help()), e.getMessage());
        }
    }

    @Test
    public void config_file_unrecognised_option() {
        Path config_unrecognised_option = Util.getTypedbDir().resolve("server/test/config_unrecognised_option.yml");
        try {
            (new Configuration.Parser()).parse(config_unrecognised_option, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(UNRECOGNISED_CONFIGURATION_OPTIONS.code(), e.code().get());
            assertEquals(UNRECOGNISED_CONFIGURATION_OPTIONS.message(list("log.custom-logger-invalid")), e.getMessage());
        }
    }

    @Test
    public void default_file_overrides_accepted() {
        Configuration configuration = (new Configuration.Parser()).getDefault(set(
                new CommandLine.Option("data", "server/alt-data"),
                new CommandLine.Option("port", "1730"),
                new CommandLine.Option("log.output.dir.path", "server/alt-logs"),
                new CommandLine.Option("log.logger.default.level", "info"),
                new CommandLine.Option("log.logger.typedb.output", "[dir]")
        ));
        assertTrue(configuration.dataDir().toString().endsWith("server/alt-data"));
        assertEquals(1730, configuration.port());
        assertFalse(configuration.vaticleFactory().trace());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("dir"));
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().path().toString().endsWith("server/alt-logs"));
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFileMB() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesCount() > 0);
        assertTrue(configuration.log().output().outputs().get("dir").asDirectory().maxFilesGB() > 0);
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("info", configuration.log().logger().defaultLogger().level());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }
}


