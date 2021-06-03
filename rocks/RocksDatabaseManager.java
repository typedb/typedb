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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_MANAGER_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NAME_RESERVED;

public class RocksDatabaseManager implements TypeDB.DatabaseManager {

    protected static final String RESERVED_NAME_PREFIX = "_";

    protected final RocksTypeDB typedb;
    protected final ConcurrentMap<String, RocksDatabase> databases;
    protected final Factory.Database databaseFactory;
    protected final AtomicBoolean isOpen;

    protected RocksDatabaseManager(RocksTypeDB typedb, Factory.Database databaseFactory) {
        this.typedb = typedb;
        this.databaseFactory = databaseFactory;
        databases = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
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
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        return databases.containsKey(name);
    }

    @Override
    public RocksDatabase create(String name) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        if (databases.containsKey(name)) throw TypeDBException.of(DATABASE_EXISTS, name);

        RocksDatabase database = databaseFactory.databaseCreateAndOpen(typedb, name);
        databases.put(name, database);
        return database;
    }

    @Override
    public RocksDatabase get(String name) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        return databases.get(name);
    }

    @Override
    public Set<RocksDatabase> all() {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        return databases.values().stream().filter(database -> !isReservedName(database.name())).collect(Collectors.toSet());
    }

    void remove(RocksDatabase database) {
        databases.remove(database.name());
    }

    protected void close() {
        if (isOpen.compareAndSet(true, false)) {
            databases.values().parallelStream().forEach(RocksDatabase::close);
        }
    }

    protected boolean isReservedName(String name) {
        return name.startsWith(RESERVED_NAME_PREFIX);
    }
}
