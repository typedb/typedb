/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.database;

import com.google.ortools.Loader;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.diagnostics.CoreDiagnostics;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import io.sentry.ITransaction;
import io.sentry.Sentry;
import io.sentry.SpanStatus;
import io.sentry.TransactionContext;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_MANAGER_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NAME_RESERVED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.TYPEDB_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNKNOWN_ERROR;
import static java.util.concurrent.TimeUnit.HOURS;

public class CoreDatabaseManager implements TypeDB.DatabaseManager {

    static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private static final long DIAGNOSTICS_DB_PERIOD = HOURS.toMillis(24);

    static {
        RocksDB.loadLibrary();
        Loader.loadNativeLibraries();
        ErrorMessage.loadConstants();
    }

    protected static final String RESERVED_NAME_PREFIX = "_";

    private final Options.Database databaseOptions;
    protected final ConcurrentMap<String, CoreDatabase> databases;
    protected final Factory.Database databaseFactory;
    protected final AtomicBoolean isOpen;
    private final ScheduledFuture<?> scheduledDiagnostics;

    public static CoreDatabaseManager open(Path directory, Factory factory) {
        return open(new Options.Database().dataDir(directory), factory);
    }

    public static CoreDatabaseManager open(Options.Database databaseOptions) {
        return open(databaseOptions, new CoreFactory());
    }

    public static CoreDatabaseManager open(Options.Database databaseOptions, Factory factory) {
        return factory.databaseManager(databaseOptions);
    }

    protected CoreDatabaseManager(Options.Database databaseOptions, Factory.Database databaseFactory) {
        if (!Executors.isInitialised()) Executors.initialise(MAX_THREADS);
        this.databaseOptions = databaseOptions;
        this.databaseFactory = databaseFactory;
        databases = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
        loadAll();
        this.scheduledDiagnostics = CoreDiagnostics.scheduledRunner(
                CoreDiagnostics.INITIAL_DELAY_MILLIS, DIAGNOSTICS_DB_PERIOD,
                "db_statistics", "db_statistics",
                null, this::submitDiagnostics, Executors.scheduled()
        );
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    protected void loadAll() {
        File[] databaseDirectories = directory().toFile().listFiles(File::isDirectory);
        if (databaseDirectories != null && databaseDirectories.length > 0) {
            List<CompletableFuture<Void>> dbLoads = Arrays.stream(databaseDirectories)
                    .filter(file -> CoreDatabase.isExistingDatabaseDirectory(file.toPath()))
                    .map(directory ->
                            CompletableFuture.runAsync(() -> {
                                String name = directory.getName();
                                CoreDatabase database = databaseFactory.databaseLoadAndOpen(this, name);
                                databases.put(name, database);
                            }, Executors.async1())
                    ).collect(Collectors.toList());
            try {
                // once all future complete, we will catch any exceptions in order to close all databases cleanly
                CompletableFuture.allOf(dbLoads.toArray(new CompletableFuture[0])).join();
            } catch (CompletionException e) {
                close();
                if (e.getCause() instanceof TypeDBException) throw (TypeDBException) e.getCause();
                else throw TypeDBException.of(UNKNOWN_ERROR, e);
            }
        }
    }

    private void submitDiagnostics(TransactionContext context) {
        for (CoreDatabase database : all()) {
            ITransaction transaction = Sentry.startTransaction(context);
            try {
                transaction.setData("db_name_hash", database.name().hashCode());
                transaction.setMeasurement("concept_type_count", database.typeCount());
                transaction.setMeasurement("concept_thing_count", database.thingCount());
                transaction.setMeasurement("concept_thing_connection_count", database.roleCount() + database.hasCount());
                transaction.setMeasurement("concept_thing_entity_count", database.entityCount());
                transaction.setMeasurement("concept_thing_relation_count", database.relationCount());
                transaction.setMeasurement("concept_thing_attribute_count", database.attributeCount());
                transaction.setMeasurement("concept_thing_role_count", database.roleCount());
                transaction.setMeasurement("concept_thing_has_count", database.hasCount());
                transaction.setMeasurement("storage_data_keys_count", database.storageDataKeysEstimate());
                transaction.setMeasurement("storage_data_bytes", database.storageDataBytesEstimate());
                transaction.setStatus(SpanStatus.OK);
            } catch (Exception e) {
                transaction.setThrowable(e);
                transaction.setStatus(SpanStatus.UNKNOWN);
            } finally {
                transaction.finish();
            }
        }
    }

    @Override
    public boolean contains(String name) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        return databases.containsKey(name);
    }

    @Override
    public CoreDatabase create(String name) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        if (databases.containsKey(name)) throw TypeDBException.of(DATABASE_EXISTS, name);

        CoreDatabase database = databaseFactory.databaseCreateAndOpen(this, name);
        databases.put(name, database);
        return database;
    }

    @Override
    public CoreDatabase get(String name) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        if (isReservedName(name)) throw TypeDBException.of(DATABASE_NAME_RESERVED);
        return databases.get(name);
    }

    @Override
    public Set<CoreDatabase> all() {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_MANAGER_CLOSED);
        return databases.values().stream().filter(database -> !isReservedName(database.name())).collect(Collectors.toSet());
    }

    void remove(CoreDatabase database) {
        databases.remove(database.name());
    }

    @Override
    public CoreSession session(String database, Arguments.Session.Type type) {
        return session(database, type, new Options.Session());
    }

    @Override
    public CoreSession session(String database, Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw TypeDBException.of(TYPEDB_CLOSED);
        if (contains(database)) return get(database).createAndOpenSession(type, options);
        else throw TypeDBException.of(DATABASE_NOT_FOUND, database);
    }

    public Path directory() {
        return databaseOptions.dataDir();
    }

    public Options.Database options() {
        return databaseOptions;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            if (scheduledDiagnostics != null) scheduledDiagnostics.cancel(true);
            databases.values().parallelStream().forEach(CoreDatabase::close);
        }
    }

    protected static boolean isReservedName(String name) {
        return name.startsWith(RESERVED_NAME_PREFIX);
    }
}
