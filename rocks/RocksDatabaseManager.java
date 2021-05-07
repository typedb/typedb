/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;

public class RocksDatabaseManager implements TypeDB.DatabaseManager {

    protected final RocksTypeDB typedb;
    protected final ConcurrentMap<String, RocksDatabase> databases;
    protected final Factory.Database databaseFactory;

    protected RocksDatabaseManager(RocksTypeDB typedb, Factory.Database databaseFactory) {
        this.typedb = typedb;
        this.databaseFactory = databaseFactory;
        databases = new ConcurrentHashMap<>();
    }

    protected void loadAll() {
        File[] databaseDirectories = typedb.directory().toFile().listFiles(File::isDirectory);
        if (databaseDirectories != null && databaseDirectories.length > 0) {
            Arrays.stream(databaseDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                RocksDatabase database = databaseFactory.databaseLoadAndOpen(typedb, name);
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
        if (databases.containsKey(name)) throw TypeDBException.of(DATABASE_EXISTS, name);

        RocksDatabase database = databaseFactory.databaseCreateAndOpen(typedb, name);
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

    protected void close() {
        all().parallelStream().forEach(RocksDatabase::close);
    }
}
