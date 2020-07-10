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

package grakn.rocks;

import grakn.Grakn;
import grakn.common.exception.GraknException;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksDatabaseManager implements Grakn.DatabaseManager {

    private final RocksGrakn core;
    private final ConcurrentMap<String, RocksDatabase> databases;

    RocksDatabaseManager(RocksGrakn core) {
        this.core = core;
        databases = new ConcurrentHashMap<>();
    }

    void loadAll() {
        File[] databaseDirectories = core.directory().toFile().listFiles(File::isDirectory);
        if (databaseDirectories != null && databaseDirectories.length > 0) {
            Arrays.stream(databaseDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                RocksDatabase database = RocksDatabase.loadExistingAndOpen(core, name);
                databases.put(name, database);
            });
        }
    }

    @Override
    public boolean contains(String database) {
        return databases.containsKey(database);
    }

    @Override
    public RocksDatabase create(String name) {
        if (databases.containsKey(name)) throw new GraknException("Database Already Exist: " + name);

        RocksDatabase database = RocksDatabase.createNewAndOpen(core, name);
        databases.put(name, database);
        return database;
    }

    @Override
    public RocksDatabase get(String database) {
        return databases.get(database);
    }

    @Override
    public Set<RocksDatabase> getAll() {
        return new HashSet<>(databases.values());
    }

    void remove(RocksDatabase database) {
        databases.remove(database.name());
    }
}
