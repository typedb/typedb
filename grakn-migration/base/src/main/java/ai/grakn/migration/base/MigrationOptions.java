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

package ai.grakn.migration.base;

import ai.grakn.Grakn;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.file.Path;
import java.nio.file.Paths;

import static ai.grakn.migration.base.MigrationCLI.die;
import static java.lang.Integer.parseInt;

/**
 * Configure the default migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class MigrationOptions {

    private static final String batch = Integer.toString(Migrator.BATCH_SIZE);
    private static final String active = Integer.toString(Migrator.ACTIVE_TASKS);
    private static final String uri = Grakn.DEFAULT_URI;
    private int numberOptions;

    protected final Options options = new Options();
    protected CommandLine command;

    public MigrationOptions(){
        options.addOption("v", "verbose", false, "Print counts of migrated data.");
        options.addOption("h", "help", false, "Print usage message.");
        options.addOption("k", "keyspace", true, "Grakn graph. Required.");
        options.addOption("u", "uri", true, "Location of Grakn Engine.");
        options.addOption("n", "no", false, "Write to standard out.");
        options.addOption("c", "config", true, "Configuration file.");
        options.addOption("r", "retry", true, "Retry sending tasks if engine is not available");
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
        if(!command.hasOption("k")){
            die("Keyspace missing (-k)");
        }

        return command.getOptionValue("k");
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

    public boolean getRetry(){
        return command.hasOption("r") && Boolean.getBoolean(command.getOptionValue("r"));
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }

    public int getNumberActiveTasks() {
        return parseInt(command.getOptionValue("a", active));
    }

    protected void parse(String[] args){
        try {
            CommandLineParser parser = new DefaultParser();
            command = parser.parse(options, args);
            numberOptions = command.getOptions().length;
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
