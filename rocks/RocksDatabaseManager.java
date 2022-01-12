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

import com.google.ortools.Loader;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_MANAGER_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NAME_RESERVED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.TYPEDB_CLOSED;

public class RocksDatabaseManager implements TypeDB.DatabaseManager {

    static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    static {
        RocksDB.loadLibrary();
        Loader.loadNativeLibraries();
        ErrorMessage.loadConstants();
    }

    protected static final String RESERVED_NAME_PREFIX = "_";

    private final Options.Database typeDBOptions;
    protected final ConcurrentMap<String, RocksDatabase> databases;
    protected final Factory.Database databaseFactory;
    protected final AtomicBoolean isOpen;

    public static RocksDatabaseManager open(Path directory, Factory typeDBFactory) {
        return open(new Options.Database().dataDir(directory), typeDBFactory);
    }

    public static RocksDatabaseManager open(Options.Database options) {
        return open(options, new RocksFactory());
    }

    public static RocksDatabaseManager open(Options.Database options, Factory typeDBFactory) {
        return typeDBFactory.databaseManager(options);
    }

    protected RocksDatabaseManager(Options.Database options, Factory.Database databaseFactory) {
        if (!Executors.isInitialised()) Executors.initialise(MAX_THREADS);
        this.typeDBOptions = options;
        this.databaseFactory = databaseFactory;
        databases = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
        loadAll();
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    protected void loadAll() {
        File[] databaseDirectories = directory().toFile().listFiles(File::isDirectory);
        if (databaseDirectories != null && databaseDirectories.length > 0) {
            Arrays.stream(databaseDirectories).parallel().forEach(directory -> {
                String name = directory.getName();
                RocksDatabase database = databaseFactory.databaseLoadAndOpen(this, name);
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

        RocksDatabase database = databaseFactory.databaseCreateAndOpen(this, name);
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

    @Override
    public RocksSession session(String database, Arguments.Session.Type type) {
        return session(database, type, new Options.Session());
    }

    @Override
    public RocksSession session(String database, Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw TypeDBException.of(TYPEDB_CLOSED);
        if (contains(database)) return get(database).createAndOpenSession(type, options);
        else throw TypeDBException.of(DATABASE_NOT_FOUND, database);
    }

    public Path directory() {
        return typeDBOptions.dataDir();
    }

    public Options.Database options() {
        return typeDBOptions;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            databases.values().parallelStream().forEach(RocksDatabase::close);
        }
    }

    protected static boolean isReservedName(String name) {
        return name.startsWith(RESERVED_NAME_PREFIX);
    }
}
