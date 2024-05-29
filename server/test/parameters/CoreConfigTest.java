/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.util.Option;
import com.vaticle.typedb.core.server.parameters.util.YAMLParser;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIGS_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_KEY_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_LOG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_VALUE_UNEXPECTED;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CoreConfigTest {

    private static final Path CONFIG_PATH_DEFAULT = Paths.get("./server/parameters/config.yml");

    @Test
    public void config_file_is_read() {
        CoreConfig config = CoreConfigFactory.config(CONFIG_PATH_DEFAULT, emptySet(), new CoreConfigParser());
        assertTrue(config.storage().dataDir().toString().endsWith("server/data"));
        assertEquals(new InetSocketAddress("0.0.0.0", 1729), config.server().address());
        assertEquals(500 * Bytes.MB, config.storage().databaseCache().dataSize());
        assertEquals(500 * Bytes.MB, config.storage().databaseCache().indexSize());
        assertFalse(config.vaticleFactory().enabled());
        assertTrue(config.log().output().outputs().containsKey("stdout"));
        assertTrue(config.log().output().outputs().containsKey("file"));
        assertTrue(config.log().output().outputs().get("file").asFile().baseDirectory().toString().endsWith("server/logs"));
        assertEquals(50 * Bytes.MB, config.log().output().outputs().get("file").asFile().fileSizeLimit());
        assertEquals(1 * Bytes.GB, config.log().output().outputs().get("file").asFile().archivesSizeLimit());
        assertNotNull(config.log().logger().defaultLogger());
        assertFalse(config.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", config.log().logger().defaultLogger().level());
        assertFalse(config.log().debugger().reasonerTracer().enabled());
        assertFalse(config.log().debugger().reasonerPerfCounters().enabled());
        assertTrue(config.developmentMode().enabled());
    }

    @Test
    public void minimal_config_with_absolute_paths_is_read() {
        Path configMinimalAbsPaths = Paths.get("./server/test/parameters/config/config-minimal-abs-path.yml");
        CoreConfig config = CoreConfigFactory.config(configMinimalAbsPaths, new HashSet<>(), new CoreConfigParser());
        assertTrue(config.storage().dataDir().isAbsolute());
        assertEquals(new InetSocketAddress("0.0.0.0", 1730), config.server().address());
        assertEquals(200 * Bytes.MB, config.storage().databaseCache().dataSize());
        assertEquals(700 * Bytes.MB, config.storage().databaseCache().indexSize());
        assertFalse(config.vaticleFactory().enabled());
        assertTrue(config.log().output().outputs().containsKey("stdout"));
        assertTrue(config.log().output().outputs().containsKey("file"));
        assertTrue(config.log().output().outputs().get("file").asFile().baseDirectory().isAbsolute());
        assertEquals(50 * Bytes.MB, config.log().output().outputs().get("file").asFile().fileSizeLimit());
        assertEquals(1 * Bytes.GB, config.log().output().outputs().get("file").asFile().archivesSizeLimit());
        assertNotNull(config.log().logger().defaultLogger());
        assertFalse(config.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("warn", config.log().logger().defaultLogger().level());
        assertFalse(config.log().debugger().reasonerTracer().enabled());
        assertFalse(config.log().debugger().reasonerPerfCounters().enabled());
    }

    @Test
    public void config_invalid_path_throws() {
        Path configMissing = Paths.get("server/test/missing.yml");
        try {
            CoreConfigFactory.config(configMissing, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIG_FILE_NOT_FOUND.code(), e.errorMessage().code());
        }
    }

    @Test
    public void config_file_missing_data_throws() {
        Path configMissingLog = Paths.get("server/test/parameters/config/config-missing-data.yml");
        try {
            CoreConfigFactory.config(configMissingLog, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIG_KEY_MISSING.code(), e.errorMessage().code());
            assertEquals(CONFIG_KEY_MISSING.message("storage.data"), e.getMessage());
        }
    }

    @Test
    public void config_file_missing_debugger_throws() {
        Path configMissingLogDebugger = Paths.get("server/test/parameters/config/config-missing-debugger.yml");
        try {
            CoreConfigFactory.config(configMissingLogDebugger, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIG_KEY_MISSING.code(), e.errorMessage().code());
            assertEquals(CONFIG_KEY_MISSING.message("log.debugger"), e.getMessage());
        }
    }

    @Test
    public void config_file_invalid_output_reference_throws() {
        Path configInvalidOutput = Paths.get("server/test/parameters/config/config-invalid-logger-output.yml");
        try {
            CoreConfigFactory.config(configInvalidOutput, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIG_LOG_OUTPUT_UNRECOGNISED.code(), e.errorMessage().code());
        }
    }

    @Test
    public void config_file_wrong_path_type_throws() {
        Path configInvalidPathType = Paths.get("server/test/parameters/config/config-wrong-path-type.yml");
        try {
            CoreConfigFactory.config(configInvalidPathType, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIG_VALUE_UNEXPECTED.code(), e.errorMessage().code());
            assertEquals(CONFIG_VALUE_UNEXPECTED.message("storage.data", "123456[int]", YAMLParser.Value.Primitive.PATH.description()), e.getMessage());
        }
    }

    @Test
    public void config_file_unrecognised_option() {
        Path configUnrecognisedOption = Paths.get("server/test/parameters/config/config-unrecognised-option.yml");
        try {
            CoreConfigFactory.config(configUnrecognisedOption, new HashSet<>(), new CoreConfigParser());
            fail();
        } catch (TypeDBException e) {
            assertEquals(CONFIGS_UNRECOGNISED.code(), e.errorMessage().code());
            assertEquals(CONFIGS_UNRECOGNISED.message(list("log.custom-logger-invalid")), e.getMessage());
        }
    }

    @Test
    public void config_file_accepts_overrides() {
        CoreConfig config = CoreConfigFactory.config(
                CONFIG_PATH_DEFAULT,
                set(
                        new Option("storage.data", "server/alt-data"),
                        new Option("server.address", "0.0.0.0:1730"),
                        new Option("log.output.file.base-dir", "server/alt-logs"),
                        new Option("log.logger.default.level", "info"),
                        new Option("log.logger.typedb.output", "[file]")
                ),
                new CoreConfigParser()
        );
        assertTrue(config.storage().dataDir().toString().endsWith("server/alt-data"));
        assertEquals(new InetSocketAddress("0.0.0.0", 1730), config.server().address());
        assertFalse(config.vaticleFactory().enabled());
        assertTrue(config.log().output().outputs().containsKey("stdout"));
        assertTrue(config.log().output().outputs().containsKey("file"));
        assertTrue(config.log().output().outputs().get("file").asFile().baseDirectory().toString().endsWith("server/alt-logs"));
        assertEquals(50 * Bytes.MB, config.log().output().outputs().get("file").asFile().fileSizeLimit());
        assertEquals(1 * Bytes.GB, config.log().output().outputs().get("file").asFile().archivesSizeLimit());
        assertNotNull(config.log().logger().defaultLogger());
        assertFalse(config.log().logger().defaultLogger().outputs().isEmpty());
        assertEquals("info", config.log().logger().defaultLogger().level());
        assertEquals(list("file"), config.log().logger().filteredLoggers().get("typedb").outputs());
        assertFalse(config.log().debugger().reasonerTracer().enabled());
        assertFalse(config.log().debugger().reasonerPerfCounters().enabled());
    }

    @Test
    public void overrides_list_can_be_yaml_or_repeated() {
        CoreConfig config = CoreConfigFactory.config(
                CONFIG_PATH_DEFAULT,
                set(new Option("log.logger.typedb.output", "[file]")),
                new CoreConfigParser()
        );
        assertEquals(set("file"), set(config.log().logger().filteredLoggers().get("typedb").outputs()));

        CoreConfig configWithRepeatedArgs = CoreConfigFactory.config(
                CONFIG_PATH_DEFAULT,
                set(
                        new Option("log.logger.typedb.output", "file"),
                        new Option("log.logger.typedb.output", "stdout")
                ),
                new CoreConfigParser()
        );
        assertEquals(set("stdout", "file"), set(configWithRepeatedArgs.log().logger().filteredLoggers().get("typedb").outputs()));
    }

    @Test
    public void development_mode_disabled_explicitly() {
        Path configPaths = Paths.get("./server/test/parameters/config/config-disabled-development-mode-explicitly.yml");
        CoreConfig config = CoreConfigFactory.config(configPaths, new HashSet<>(), new CoreConfigParser());
        assertFalse(config.developmentMode().enabled());
    }

    @Test
    public void development_mode_disabled_implicitly() {
        Path configPaths = Paths.get("./server/test/parameters/config/config-disabled-development-mode-implicitly.yml");
        CoreConfig config = CoreConfigFactory.config(configPaths, new HashSet<>(), new CoreConfigParser());
        assertFalse(config.developmentMode().enabled());
    }
}
