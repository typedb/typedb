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

package ai.grakn.migration.base.io;

import ai.grakn.Grakn;
import ai.grakn.engine.util.ConfigProperties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.file.Path;
import java.nio.file.Paths;

import static ai.grakn.migration.base.io.MigrationCLI.die;

/**
 * Configure the default migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class MigrationOptions {
    private static final ConfigProperties properties = ConfigProperties.getInstance();

    private static final String keyspace = properties.getProperty(ConfigProperties.DEFAULT_KEYSPACE_PROPERTY);
    private static final String uri = Grakn.DEFAULT_URI;
    private int numberOptions;

    protected final Options options = new Options();
    protected CommandLine command;

    public MigrationOptions(String[] args){
        options.addOption("v", "verbose", false, "Print counts of migrated data.");
        options.addOption("h", "help", false, "Print usage message.");
        options.addOption("k", "keyspace", true, "Grakn graph.");
        options.addOption("u", "uri", true, "Location of Grakn Engine.");
        options.addOption("n", "no", false, "Write to standard out.");
        options.addOption("c", "config", true, "Configuration file.");
    }

    public boolean isVerbose() {
        return command.hasOption("v");
    }

    public boolean isHelp() {
        return command.hasOption("h");
    }

    public boolean isNo() {
        return command.hasOption("n");
    }

    public String getKeyspace() {
        return command.hasOption("k") ? command.getOptionValue("k") : keyspace;
    }

    public String getConfiguration() {
        return command.hasOption("c") ? command.getOptionValue("c") : null;
    }

    public String getUri() {
        return command.hasOption("u") ? command.getOptionValue("u") : uri;
    }

    public Options getOptions(){
        return options;
    }

    public int getNumberOptions() {
        return numberOptions;
    }

    public String getInput() {
        if(!command.hasOption("i")){
            die("Data file missing (-i)");
        }

        return resolvePath(command.getOptionValue("i"));
    }

    public String getTemplate() {
        if(!command.hasOption("t")){
            die("Template file missing (-t)");
        }

        return resolvePath(command.getOptionValue("t"));
    }

    protected <T extends MigrationOptions> T parse(String[] args){
        try {
            CommandLineParser parser = new DefaultParser();
            command = parser.parse(options, args);
            numberOptions = command.getOptions().length;

            return (T) this;
        } catch (ParseException e){
            throw new RuntimeException(e);
        }
    }

    private String resolvePath(String path){
        Path givenPath = Paths.get(path);
        if(givenPath.isAbsolute()){
            return givenPath.toAbsolutePath().toString();
        }

        return Paths.get("").toAbsolutePath().resolve(givenPath).toString();
    }
}
