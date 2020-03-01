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
 */

package grakn.core.graph.diskstorage.util;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.PermanentBackendException;

import java.io.File;

/**
 * Utility methods for dealing with directory structures that are not provided by Apache Commons.
 */

public class DirectoryUtil {

    public static File getOrCreateDataDirectory(String location) throws BackendException {
        return getOrCreateDataDirectory(location, null);
    }

    public static File getOrCreateDataDirectory(String location, String childLocation) throws BackendException {
        final File storageDir;
        if (null != childLocation) {
            storageDir = new File(location, childLocation);
        } else {
            storageDir = new File(location);
        }
        if (storageDir.exists() && storageDir.isFile()) {
            throw new PermanentBackendException(String.format("%s exists but is a file.", location));
        }
        if (!storageDir.exists() && !storageDir.mkdirs()) {
            throw new PermanentBackendException(String.format("Failed to create directory %s for local storage.", location));
        }
        return storageDir;
    }

}
