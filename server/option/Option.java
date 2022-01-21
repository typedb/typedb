package com.vaticle.typedb.core.server.option;

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
import com.vaticle.typedb.core.server.option.cli.Command;
import com.vaticle.typedb.core.server.option.cli.CommandLine;
import com.vaticle.typedb.core.server.option.conf.Config;
import com.vaticle.typedb.core.server.option.conf.ConfigParser;

import java.util.HashMap;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE_ARCHIVE_SUFFIX;

public class Option {
    public static CommandLine commandLine() {
        return new CommandLine()
                .command(new Command.Server.Parser(new ConfigParser()))
                .command(new Command.Import.Parser())
                .command(new Command.Export.Parser());
    }

    public static void configureLogback(LoggerContext context, Config config) {
        // all appenders use the same layout
        LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
        encoder.setContext(context);
        TTLLLayout layout = new TTLLLayout(); // "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        layout.setContext(context);
        layout.start();
        encoder.setLayout(layout);

        Map<String, Appender<ILoggingEvent>> appenders = new HashMap<>();
        config.log().output().outputs().forEach((name, outputType) -> {
            if (outputType.isStdout()) {
                appenders.put(name, consoleAppender(name, context, encoder, layout));
            } else if (outputType.isFile()) {
                appenders.put(name, fileAppender(name, context, encoder, layout, outputType.asFile()));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        });

        configureRootLogger(config.log().logger().defaultLogger(), context, appenders);
        config.log().logger().filteredLoggers().values().forEach(l -> configureLogger(l, context, appenders));
    }

    private static void configureLogger(Config.Log.Logger.Filtered logger, LoggerContext context,
                                        Map<String, Appender<ILoggingEvent>> appenders) {
        Logger log = context.getLogger(logger.filter());
        log.setAdditive(false);
        log.setLevel(Level.valueOf(logger.level()));
        logger.outputs().forEach(outputName -> {
            assert appenders.containsKey(outputName);
            log.addAppender(appenders.get(outputName));
        });
    }

    private static void configureRootLogger(Config.Log.Logger.Unfiltered defaultLogger, LoggerContext context,
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
                                                                   TTLLLayout layout, Config.Log.Output.Type.File outputType) {
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
