/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.util.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;

/**
 * IO Utility class
 */
public class IOUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

    static public boolean deleteDirectory(File path, boolean includeDir) {
        boolean success = true;
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    success = deleteDirectory(file, true) && success;
                } else {
                    success = file.delete() && success;
                }
            }
        }
        if (includeDir) success = path.delete() && success;
        return success;
    }

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            LOGGER.warn("Failed closing " + c, e);
        }
    }

    public static void closeQuietly(AutoCloseable c) {

        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            LOGGER.warn("Failed closing " + c, e);
        }
    }
}
