/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.rocks;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;

public class RocksDatabaseManager implements Grakn.DatabaseManager {

    protected final RocksGrakn grakn;
    protected final ConcurrentMap<String, RocksDatabase> databases;
    private final Factory.Database databaseFactory;

    protected RocksDatabaseManager(RocksGrakn grakn, Factory.Database databaseFactory) {
        this.grakn = grakn;
        this.databaseFactory = databaseFactory;
        databases = new ConcurrentHashMap<>();
    }

    protected void loadAll() {
        File[] databaseDirectories = grakn.directory().toFile().listFiles(File::isDirectory);
        if (databaseDirectories != null && databaseDirectories.length > 0) {
            Arrays.stream(databaseDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                RocksDatabase database = databaseFactory.databaseLoadAndOpen(grakn, name);
                databases.put(name, database);
            });
        }
    }

    @Override
    public boolean contains(String name) {
        return databases.containsKey(name);
    }

    @Override
    public RocksDatabase create(String name) {
        if (databases.containsKey(name)) throw GraknException.of(DATABASE_EXISTS, name);

        RocksDatabase database = databaseFactory.databaseCreateAndOpen(grakn, name);
        databases.put(name, database);
        return database;
    }

    @Override
    public RocksDatabase get(String name) {
        return databases.get(name);
    }

    @Override
    public Set<RocksDatabase> all() {
        return new HashSet<>(databases.values());
    }

    void remove(RocksDatabase database) {
        databases.remove(database.name());
    }
}
