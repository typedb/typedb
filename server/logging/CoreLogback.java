/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.logging;

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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.CoreConfig;

import java.util.HashMap;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE_ARCHIVE_SUFFIX;

public class CoreLogback {

    public void configure(LoggerContext logContext, CoreConfig.Log logConfig) {
        // all appenders use the same layout
        LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
        encoder.setContext(logContext);
        TTLLLayout layout = new TTLLLayout(); // "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        layout.setContext(logContext);
        layout.start();
        encoder.setLayout(layout);

        Map<String, Appender<ILoggingEvent>> appenders = new HashMap<>();
        logConfig.output().outputs().forEach((name, outputType) ->
                appenders.put(name, appender(name, logContext, encoder, layout, outputType)));

        configureRootLogger(logConfig.logger().defaultLogger(), logContext, appenders);
        logConfig.logger().filteredLoggers().values().forEach(l -> configureLogger(l, logContext, appenders));
    }

    protected Appender<ILoggingEvent> appender(String name,
                                               LoggerContext logContext,
                                               LayoutWrappingEncoder<ILoggingEvent> encoder,
                                               TTLLLayout layout,
                                               CoreConfig.Log.Output.Type outputType) {
        if (outputType.isStdout()) {
            return consoleAppender(name, logContext, encoder, layout);
        } else if (outputType.isFile()) {
            return fileAppender(name, logContext, encoder, layout, outputType.asFile());
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static void configureLogger(CoreConfig.Log.Logger.Filtered logger, LoggerContext context,
                                        Map<String, Appender<ILoggingEvent>> appenders) {
        Logger log = context.getLogger(logger.filter());
        log.setAdditive(false);
        log.setLevel(Level.valueOf(logger.level()));
        logger.outputs().forEach(outputName -> {
            assert appenders.containsKey(outputName);
            log.addAppender(appenders.get(outputName));
        });
    }

    private static void configureRootLogger(CoreConfig.Log.Logger.Unfiltered defaultLogger, LoggerContext context,
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

    protected static ConsoleAppender<ILoggingEvent> consoleAppender(String name, LoggerContext context,
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

    protected static RollingFileAppender<ILoggingEvent> fileAppender(String name, LoggerContext context,
                                                                   LayoutWrappingEncoder<ILoggingEvent> encoder,
                                                                   TTLLLayout layout, CoreConfig.Log.Output.Type.File outputType) {
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
        long directorySize = outputType.fileSizeCap() + outputType.archivesSizeCap();
        policy.setMaxFileSize(new FileSize(outputType.fileSizeCap()));
        policy.setTotalSizeCap(new FileSize(directorySize));
        policy.setParent(appender);
        policy.start();
        appender.setRollingPolicy(policy);
        appender.start();
        return appender;
    }
}
