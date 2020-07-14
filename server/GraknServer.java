/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server;

import grakn.core.common.exception.GraknException;
import grakn.core.server.util.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.PropertiesDefaultProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import static grakn.core.common.exception.Error.Server.ENV_VAR_NOT_EXIST;
import static grakn.core.common.exception.Error.Server.EXITED_WITH_ERROR;
import static grakn.core.common.exception.Error.Server.FAILED_PARSE_PROPERTIES;
import static grakn.core.common.exception.Error.Server.PROPERTIES_FILE_NOT_AVAILABLE;


public class GraknServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GraknServer.class);
    private Options options;

    GraknServer(Options options) {
        this.options = options;
    }

    public int run() {
        return 0;
    }

    @Override
    public void close() {

    }

    private static Options parseOptions(Properties properties, String[] args) {
        Options options = new Options();
        CommandLine cmd = new CommandLine(options);
        cmd.setDefaultValueProvider(new PropertiesDefaultProvider(properties));
        cmd.parseArgs(args);
        return options;
    }

    private static Properties parseProperties() {
        Properties properties = new Properties();
        boolean error = false;
        File file = Paths.get(Options.DEAFAULT_PROPERTIES_FILE).toFile();

        try {
            properties.load(new FileInputStream(file));
        } catch (IOException e) {
            LOG.error(PROPERTIES_FILE_NOT_AVAILABLE.message(file.toString()));
            error = true;
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            if (val.startsWith("$")) {
                String envVarName = val.substring(1);
                if (System.getenv(envVarName) == null) {
                    LOG.error(ENV_VAR_NOT_EXIST.message(val));
                    error = true;
                } else {
                    properties.put(key, System.getenv(envVarName));
                }
            }
        }

        if (error) throw new GraknException(FAILED_PARSE_PROPERTIES);
        else return properties;
    }

    public static void main(String[] args) {
        int status = 0;
        boolean error = false;

        try {
            GraknServer server = new GraknServer(parseOptions(parseProperties(), args));
            status = server.run();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            error = true;
        }

        if (status != 0 || error) LOG.error(EXITED_WITH_ERROR.message(status));
        System.exit(status);
    }
}
