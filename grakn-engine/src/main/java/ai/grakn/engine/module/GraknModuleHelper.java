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
 *
 */

package ai.grakn.engine.module;

import ai.grakn.graknmodule.GraknModule;
import ai.grakn.graknmodule.http.Before;
import ai.grakn.graknmodule.http.BeforeHttpEndpoint;
import ai.grakn.graknmodule.http.HttpEndpoint;
import ai.grakn.graknmodule.http.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Filter;
import spark.Request;
import spark.Route;
import spark.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static spark.Spark.halt;

/**
 * Grakn Module helper
 *
 * @author Ganeshwara Herawan Hananda
 */
public class GraknModuleHelper {
    public static Stream<Path> listFolders(Path directory) {
        try {
            return Files.list(directory)
                .filter(e -> e.toFile().isDirectory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Path> listJarFiles(Path directory) {
        try {
            return Files.list(directory)
                .filter(e -> e.toFile().isFile())
                .filter(e -> e.getFileName().toString().endsWith(".jar"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
