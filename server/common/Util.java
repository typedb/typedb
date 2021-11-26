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

package com.vaticle.typedb.core.server.common;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.layout.TTLLLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static ch.qos.logback.core.util.FileSize.GB_COEFFICIENT;
import static ch.qos.logback.core.util.FileSize.MB_COEFFICIENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static com.vaticle.typedb.core.server.common.Configuration.Log.Logger.Filtered;
import static com.vaticle.typedb.core.server.common.Configuration.Log.Logger.Unfiltered;
import static com.vaticle.typedb.core.server.common.Configuration.Log.Output.Type;
import static com.vaticle.typedb.core.server.common.Constants.ASCII_LOGO_FILE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE_ARCHIVE_SUFFIX;

public class Util {

    public static void printASCIILogo() throws IOException {
        if (ASCII_LOGO_FILE.exists()) {
            System.out.println("\n" + new String(Files.readAllBytes(ASCII_LOGO_FILE.toPath()), StandardCharsets.UTF_8));
        }
    }

    public static CommandLine commandLine() {
        return new CommandLine()
                .command(new Command.Server.Parser(new Configuration.Parser()))
                .command(new Command.Import.Parser())
                .command(new Command.Export.Parser());
    }

    public static Path getTypedbDir() {
        String homeDir;
        if ((homeDir = System.getProperty("typedb.dir")) == null) {
            homeDir = System.getProperty("user.dir");
        }
        return Paths.get(homeDir);
    }

    public static Path getConfigPath(Path relativeOrAbsolutePath) {
        if (relativeOrAbsolutePath.isAbsolute()) return relativeOrAbsolutePath;
        else {
            Path typeDBDir = getTypedbDir();
            return typeDBDir.resolve(relativeOrAbsolutePath);
        }
    }

    static Yaml.Map readConfig(Path path) {
        try {
            Yaml yaml = Yaml.load(path);
            if (!yaml.isMap()) throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP);
            replaceEnvVars(yaml.asMap());
            return yaml.asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(CONFIG_FILE_NOT_FOUND, path);
        }
    }

    private static void replaceEnvVars(Yaml.Map yaml) {
        for (String key : yaml.keys()) {
            if (yaml.get(key).isString()) {
                String value = yaml.get(key).asString().value();
                if (value.startsWith("$")) {
                    String envVarName = value.substring(1);
                    if (System.getenv(envVarName) == null) throw TypeDBException.of(ENV_VAR_NOT_FOUND, value);
                    else yaml.put(key, Yaml.load(System.getenv(envVarName)));
                }
            } else if (yaml.get(key).isMap()) {
                replaceEnvVars(yaml.get(key).asMap());
            }
        }
    }

    static String scopeKey(String scope, String key) {
        return scope.isEmpty() ? key : scope + "." + key;
    }

    static void setValue(Yaml.Map yaml, String[] path, Yaml value) {
        Yaml.Map nested = yaml.asMap();
        for (int i = 0; i < path.length - 1; i++) {
            String key = path[i];
            if (!nested.containsKey(key)) nested.put(key, new Yaml.Map(new HashMap<>()));
            nested = nested.get(key).asMap();
        }
        nested.put(path[path.length - 1], value);
    }

    public static void configureLogback(LoggerContext context, Configuration configuration) {
        // all appenders use the same layout
        LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
        encoder.setContext(context);
        TTLLLayout layout = new TTLLLayout(); // "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        layout.setContext(context);
        layout.start();
        encoder.setLayout(layout);

        Map<String, Appender<ILoggingEvent>> appenders = new HashMap<>();
        configuration.log().output().outputs().forEach((name, outputType) -> {
            if (outputType.isStdout()) {
                appenders.put(name, consoleAppender(name, context, encoder, layout));
            } else if (outputType.isFile()) {
                appenders.put(name, fileAppender(name, context, encoder, layout, outputType.asFile()));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        });

        configureRootLogger(configuration.log().logger().defaultLogger(), context, appenders);
        configuration.log().logger().filteredLoggers().values().forEach(l -> configureLogger(l, context, appenders));
    }

    private static void configureLogger(Filtered logger, LoggerContext context,
                                        Map<String, Appender<ILoggingEvent>> appenders) {
        Logger log = context.getLogger(logger.filter());
        log.setAdditive(false);
        log.setLevel(Level.valueOf(logger.level()));
        logger.outputs().forEach(outputName -> {
            assert appenders.containsKey(outputName);
            log.addAppender(appenders.get(outputName));
        });
    }

    private static void configureRootLogger(Unfiltered defaultLogger, LoggerContext context,
                                            Map<String, Appender<ILoggingEvent>> appenders) {
        Logger root = context.getLogger(Logger.ROOT_LOGGER_NAME);
        root.detachAndStopAllAppenders();
        root.setAdditive(false);
        root.setLevel(Level.valueOf(defaultLogger.level()));
        defaultLogger.outputs().forEach(outputName -> {
            assert appenders.containsKey(outputName);
            root.addAppender(appenders.get(outputName));
        });
    }

    private static RollingFileAppender<ILoggingEvent> fileAppender(String name, LoggerContext context,
                                                                   LayoutWrappingEncoder<ILoggingEvent> encoder,
                                                                   TTLLLayout layout, Type.File outputType) {
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setContext(context);
        appender.setName(name);
        appender.setAppend(true);
        String logPath = outputType.asFile().path().resolve(TYPEDB_LOG_FILE).toAbsolutePath().toString();
        appender.setFile(logPath);
        appender.setLayout(layout);
        appender.setEncoder(encoder);
        SizeAndTimeBasedRollingPolicy<?> policy = new SizeAndTimeBasedRollingPolicy<>();
        policy.setContext(context);
        policy.setFileNamePattern(logPath + TYPEDB_LOG_FILE_ARCHIVE_SUFFIX);
        FileSize fileSize = FileSize.valueOf(outputType.fileSizeCap());
        long directorySize = fileSize.getSize() + FileSize.valueOf(outputType.archivesSizeCap()).getSize();
        policy.setMaxFileSize(fileSize);
        policy.setTotalSizeCap(new FileSize(directorySize));
        policy.setParent(appender);
        policy.start();
        appender.setRollingPolicy(policy);
        appender.start();
        return appender;
    }

    private static ConsoleAppender<ILoggingEvent> consoleAppender(String name, LoggerContext context,
                                                                  LayoutWrappingEncoder<ILoggingEvent> encoder,
                                                                  TTLLLayout layout) {
        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setContext(context);
        appender.setName(name);
        appender.setEncoder(encoder);
        appender.setLayout(layout);
        appender.start();
        return appender;
    }
}
