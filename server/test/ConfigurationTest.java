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

import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.CommandLine;
import com.vaticle.typedb.core.server.common.ConfigKVParser;
import com.vaticle.typedb.core.server.common.Configuration;
import com.vaticle.typedb.core.server.common.Util;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashSet;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_UNEXPECTED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONFIG_OPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CONFIGURATION_OPTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigurationTest {

    @Test
    public void config_file_is_read() {
        Configuration configuration = (new Configuration.Parser()).getConfig();
        assertTrue(configuration.storage().dataDir().toString().endsWith("server/data"));
        assertEquals(new InetSocketAddress("0.0.0.0", 1729), configuration.server().address());
        assertEquals(500 * Bytes.MB, configuration.storage().databaseCache().dataSize());
        assertEquals(500 * Bytes.MB, configuration.storage().databaseCache().indexSize());
        assertFalse(configuration.vaticleFactory().enable());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("file"));
        assertTrue(configuration.log().output().outputs().get("file").asFile().path().toString().endsWith("server/logs"));
        assertEquals(50 * Bytes.MB, configuration.log().output().outputs().get("file").asFile().fileSizeCap());
        assertEquals(1 * Bytes.GB, configuration.log().output().outputs().get("file").asFile().archivesSizeCap());
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", configuration.log().logger().defaultLogger().level());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }

    @Test
    public void minimal_config_with_absolute_paths_is_read() {
        Path configMinimalAbsPaths = Util.getTypedbDir().resolve("server/test/config/config-minimal-abs-path.yml");
        Configuration configuration = (new Configuration.Parser()).getConfig(configMinimalAbsPaths, new HashSet<>());
        assertTrue(configuration.storage().dataDir().isAbsolute());
        assertEquals(new InetSocketAddress("0.0.0.0", 1730), configuration.server().address());
        assertEquals(200 * Bytes.MB, configuration.storage().databaseCache().dataSize());
        assertEquals(700 * Bytes.MB, configuration.storage().databaseCache().indexSize());
        assertFalse(configuration.vaticleFactory().enable());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("file"));
        assertTrue(configuration.log().output().outputs().get("file").asFile().path().isAbsolute());
        assertEquals(50 * Bytes.MB, configuration.log().output().outputs().get("file").asFile().fileSizeCap());
        assertEquals(1 * Bytes.GB, configuration.log().output().outputs().get("file").asFile().archivesSizeCap());
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", configuration.log().logger().defaultLogger().level());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }

    @Test
    public void config_invalid_path_throws() {
        Path configMissing = Util.getTypedbDir().resolve("server/test/missing.yml");
        try {
            (new Configuration.Parser()).getConfig(configMissing, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_FILE_NOT_FOUND.code(), e.code().get());
        }
    }

    @Test
    public void config_file_missing_data_throws() {
        Path configMissingLog = Util.getTypedbDir().resolve("server/test/config/config-missing-data.yml");
        try {
            (new Configuration.Parser()).getConfig(configMissingLog, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(MISSING_CONFIG_OPTION.code(), e.code().get());
            assertEquals(MISSING_CONFIG_OPTION.message("storage.data"), e.getMessage());
        }
    }

    @Test
    public void config_file_missing_debugger_throws() {
        Path configMissingLogDebugger = Util.getTypedbDir().resolve("server/test/config/config-missing-debugger.yml");
        try {
            (new Configuration.Parser()).getConfig(configMissingLogDebugger, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(MISSING_CONFIG_OPTION.code(), e.code().get());
            assertEquals(MISSING_CONFIG_OPTION.message("log.debugger"), e.getMessage());
        }
    }

    @Test
    public void config_file_invalid_output_reference_throws() {
        Path configInvalidOutput = Util.getTypedbDir().resolve("server/test/config/config-invalid-logger-output.yml");
        try {
            (new Configuration.Parser()).getConfig(configInvalidOutput, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_OUTPUT_UNRECOGNISED.code(), e.code().get());
        }
    }

    @Test
    public void config_file_wrong_path_type_throws() {
        Path configInvalidPathType = Util.getTypedbDir().resolve("server/test/config/config-wrong-path-type.yml");
        try {
            (new Configuration.Parser()).getConfig(configInvalidPathType, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(CONFIG_UNEXPECTED_VALUE.code(), e.code().get());
            assertEquals(CONFIG_UNEXPECTED_VALUE.message("storage.data", "123456[int]", ConfigKVParser.ValueParser.Leaf.PATH.help()), e.getMessage());
        }
    }

    @Test
    public void config_file_unrecognised_option() {
        Path configUnrecognisedOption = Util.getTypedbDir().resolve("server/test/config/config-unrecognised-option.yml");
        try {
            (new Configuration.Parser()).getConfig(configUnrecognisedOption, new HashSet<>());
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(UNRECOGNISED_CONFIGURATION_OPTIONS.code(), e.code().get());
            assertEquals(UNRECOGNISED_CONFIGURATION_OPTIONS.message(list("log.custom-logger-invalid")), e.getMessage());
        }
    }

    @Test
    public void config_file_accepts_overrides() {
        Configuration configuration = (new Configuration.Parser()).getConfig(set(
                new CommandLine.Option("storage.data", "server/alt-data"),
                new CommandLine.Option("server.address", "0.0.0.0:1730"),
                new CommandLine.Option("log.output.file.directory", "server/alt-logs"),
                new CommandLine.Option("log.logger.default.level", "info"),
                new CommandLine.Option("log.logger.typedb.output", "[file]")
        ));
        assertTrue(configuration.storage().dataDir().toString().endsWith("server/alt-data"));
        assertEquals(new InetSocketAddress("0.0.0.0", 1730), configuration.server().address());
        assertFalse(configuration.vaticleFactory().enable());
        assertTrue(configuration.log().output().outputs().containsKey("stdout"));
        assertTrue(configuration.log().output().outputs().containsKey("file"));
        assertTrue(configuration.log().output().outputs().get("file").asFile().path().toString().endsWith("server/alt-logs"));
        assertEquals(50 * Bytes.MB, configuration.log().output().outputs().get("file").asFile().fileSizeCap());
        assertEquals(1 * Bytes.GB, configuration.log().output().outputs().get("file").asFile().archivesSizeCap());
        assertNotNull(configuration.log().logger().defaultLogger());
        assertFalse(configuration.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("info", configuration.log().logger().defaultLogger().level());
        assertEquals(list("file"), configuration.log().logger().filteredLoggers().get("typedb").outputs());
        assertFalse(configuration.log().debugger().reasoner().isEnabled());
    }

    @Test
    public void overrides_list_can_be_yaml_or_repeated() {
        Configuration configuration = (new Configuration.Parser()).getConfig(set(
                new CommandLine.Option("log.logger.typedb.output", "[file]")
        ));
        assertEquals(set("file"), set(configuration.log().logger().filteredLoggers().get("typedb").outputs()));

        Configuration configurationWithRepeatedArgs = (new Configuration.Parser()).getConfig(set(
                new CommandLine.Option("log.logger.typedb.output", "file"),
                new CommandLine.Option("log.logger.typedb.output", "stdout")
        ));
        assertEquals(set("stdout", "file"), set(configurationWithRepeatedArgs.log().logger().filteredLoggers().get("typedb").outputs()));
    }
}
