/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

/*-
 * #%L
 * migration-base
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.Integer.parseInt;

/**
 * Configure the default migration options and access arguments passed by the user
 *
 * @author alexandraorth
 */
public class MigrationOptions {

    public static final String MAX_DELAY_DEFAULT_VALUE = "1000";
    public static final String RETRY_DEFAULT_VALUE = "5";
    public static final String LINES_DEFAULT_VALUE = "-1";
    private int numberOptions;

    protected final Options options = new Options();
    protected CommandLine command;

    public MigrationOptions() {
        options.addOption("v", "verbose", false, "Print counts of migrated data.");
        options.addOption("h", "help", false, "Print usage message.");
        options.addOption("k", "keyspace", true, "Grakn graph. Required.");
        options.addOption("u", "uri", true, "Location of Grakn Engine.");
        options.addOption("m", "maxdelay", true, "Max delay before a request is batched.");
        options.addOption("n", "no", false, "Write to standard out.");
        options.addOption("c", "config", true, "Configuration file.");
        options.addOption("r", "retry", true,
                "Number of times to retry sending tasks if engine is not available");
        options.addOption("d", "debug", false,
                "Immediately stop and fail migration if an error occurs");
        options.addOption("z", "lines", true,
                "Number of lines to be processed. Used for testing when we want to stop earlier.");
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

    public boolean isDebug() {
        return command.hasOption("d");
    }

    public Keyspace getKeyspace() {
        if (!command.hasOption("k")) {
            throw new IllegalArgumentException("Keyspace missing (-k)");
        }

        return Keyspace.of(command.getOptionValue("k"));
    }

    @Nullable
    public String getConfiguration() {
        return command.hasOption("c") ? command.getOptionValue("c") : null;
    }

    public SimpleURI getUri() {
        return command.hasOption("u") ? new SimpleURI(command.getOptionValue("u")) : Grakn.DEFAULT_URI;
    }

    public Options getOptions() {
        return options;
    }

    public int getNumberOptions() {
        return numberOptions;
    }

    public String getInput() {
        if (!command.hasOption("i")) {
            throw new IllegalArgumentException("Data file missing (-i)");
        }

        return resolvePath(command.getOptionValue("i"));
    }

    public boolean hasInput() {
        return command.hasOption("i");
    }

    public String getTemplate() {
        if (!command.hasOption("t")) {
            throw new IllegalArgumentException("Template file missing (-t)");
        }

        return resolvePath(command.getOptionValue("t"));
    }

    public int getRetry() {
        return parseInt(command.getOptionValue("r", RETRY_DEFAULT_VALUE));
    }


    public int getMaxDelay() {
        return parseInt(command.getOptionValue("m", MAX_DELAY_DEFAULT_VALUE));
    }

    public int getLines() {
        return parseInt(command.getOptionValue("z", LINES_DEFAULT_VALUE));
    }


    protected void parse(String[] args) {
        try {
            CommandLineParser parser = new DefaultParser();
            command = parser.parse(options, args);
            numberOptions = command.getOptions().length;
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
    private String resolvePath(String path) {
        Path givenPath = Paths.get(path);
        if (givenPath.isAbsolute()) {
            return givenPath.toAbsolutePath().toString();
        }

        return Paths.get("").toAbsolutePath().resolve(givenPath).toString();
    }
}
