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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * A helper class for {@link GraknModuleManager}.
 * Provides helper methods such as for accessing file systems
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
