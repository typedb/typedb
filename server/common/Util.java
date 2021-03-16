package grakn.core.server.common;

import grakn.core.common.exception.GraknException;
import grakn.core.server.GraknServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static grakn.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.FAILED_PARSE_PROPERTIES;
import static grakn.core.common.exception.ErrorMessage.Server.PROPERTIES_FILE_NOT_FOUND;
import static grakn.core.server.common.ServerDefaults.ASCII_LOGO_FILE;
import static grakn.core.server.common.ServerDefaults.PROPERTIES_FILE;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void printASCIILogo() throws IOException {
        if (ASCII_LOGO_FILE.exists()) {
            System.out.println("\n" + new String(Files.readAllBytes(ASCII_LOGO_FILE.toPath()), StandardCharsets.UTF_8));
        }
    }

    public static Properties parseProperties() {
        Properties properties = new Properties();
        boolean error = false;

        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            LOG.warn(PROPERTIES_FILE_NOT_FOUND.message(PROPERTIES_FILE.toString()));
            return new Properties();
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String val = (String) entry.getValue();
            if (val.startsWith("$")) {
                String envVarName = val.substring(1);
                if (System.getenv(envVarName) == null) {
                    LOG.error(ENV_VAR_NOT_FOUND.message(val));
                    error = true;
                } else {
                    properties.put(entry.getKey(), System.getenv(envVarName));
                }
            }
        }

        if (error) throw GraknException.of(FAILED_PARSE_PROPERTIES);
        else return properties;
    }

    public static ServerCommand parseCommandLine(Properties properties, String[] args) {
        ServerCommand.Start startCommand = new ServerCommand.Start();
        ServerCommand.ImportData importDataCommand = new ServerCommand.ImportData(startCommand);
        ServerCommand.ExportData exportDataCommand = new ServerCommand.ExportData(startCommand);
        ServerCommand.PrintSchema printSchemaCommand = new ServerCommand.PrintSchema(startCommand);
        CommandLine commandLine = new CommandLine(startCommand)
                .addSubcommand(importDataCommand)
                .addSubcommand(exportDataCommand)
                .addSubcommand(printSchemaCommand);
        commandLine.setDefaultValueProvider(new CommandLine.PropertiesDefaultProvider(properties));

        try {
            CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
            if (commandLine.isUsageHelpRequested()) {
                commandLine.usage(commandLine.getOut());
                return null;
            } else if (commandLine.isVersionHelpRequested()) {
                commandLine.printVersionHelp(commandLine.getOut());
                return null;
            } else {
                if (parseResult.hasSubcommand()) {
                    assert parseResult.subcommand().asCommandLineList().size() == 1;
                    return parseResult.subcommand().asCommandLineList().get(0).getCommand();
                } else {
                    assert parseResult.asCommandLineList().size() == 1;
                    return parseResult.asCommandLineList().get(0).getCommand();
                }
            }
        } catch (CommandLine.ParameterException ex) {
            commandLine.getErr().println(ex.getMessage());
            if (!CommandLine.UnmatchedArgumentException.printSuggestions(ex, commandLine.getErr())) {
                ex.getCommandLine().usage(commandLine.getErr());
            }
            return null;
        }
    }
}
