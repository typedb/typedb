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

package com.vaticle.typedb.core.migrator.data;

import com.google.protobuf.Parser;
import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.migrator.MigratorProto;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.IMPORT_CHECKSUM_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.MISSING_HEADER;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.NO_PLAYERS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.PLAYER_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.migrator.data.DataProto.Item.ItemCase.HEADER;
import static java.util.Comparator.reverseOrder;

public class DataImporter {
    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);

    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 1000;
    private final TypeDB.Session session;
    private final Executor importExecutor;
    private final Executor readerExecutor;
    private final int parallelisation;

    private final Path dataFile;
    private final IDMapper idMapper;
    private final String version;
    private final Status status;
    private final ConcurrentSet<DataProto.Item.Relation> skippedRelations;
    private Checksum checksum;

    public DataImporter(TypeDB.DatabaseManager typedb, String database, Path dataFile, String version) {
        if (!Files.exists(dataFile)) throw TypeDBException.of(FILE_NOT_FOUND, dataFile);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.dataFile = dataFile;
        this.version = version;
        assert com.vaticle.typedb.core.concurrent.executor.Executors.isInitialised();
        this.parallelisation = com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
        this.importExecutor = Executors.newFixedThreadPool(parallelisation);
        this.readerExecutor = Executors.newSingleThreadExecutor();
        this.idMapper = new IDMapper(database);
        this.skippedRelations = new ConcurrentSet<>();
        this.status = new Status();
    }

    public void run() {
        try {
            validateHeader();
            new ParallelImport(AttributesAndChecksum::new).execute();
            new ParallelImport(EntitiesAndOwnerships::new).execute();
            new ParallelImport(CompleteRelations::new).execute();
            importSkippedRelations();
            if (!checksum.verify(status)) throw TypeDBException.of(IMPORT_CHECKSUM_MISMATCH, checksum.mismatch(status));
        } finally {
            LOG.info("Imported " + status.toString());
        }
    }

    public void close() {
        session.close();
        idMapper.close();
    }

    private void validateHeader() {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(dataFile))) {
            DataProto.Item item = ITEM_PARSER.parseDelimitedFrom(inputStream);
            if (!item.getItemCase().equals(HEADER)) throw TypeDBException.of(MISSING_HEADER);
            DataProto.Item.Header header = item.getHeader();
            LOG.info("Importing {} from TypeDB {} to {} in TypeDB {}", header.getOriginalDatabase(),
                    header.getTypedbVersion(), session.database().name(), version);
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }
    }

    public MigratorProto.Import.Progress getProgress() {
        if (checksum != null) {
            return MigratorProto.Import.Progress.newBuilder()
                    .setInitialising(false)
                    .setAttributesCurrent(status.attributeCount.get())
                    .setEntitiesCurrent(status.entityCount.get())
                    .setRelationsCurrent(status.relationCount.get())
                    .setOwnershipsCurrent(status.ownershipCount.get())
                    .setRolesCurrent(status.roleCount.get())
                    .setAttributes(checksum.attributes)
                    .setEntities(checksum.entities)
                    .setRelations(checksum.relations)
                    .setOwnerships(checksum.ownerships)
                    .setRoles(checksum.roles)
                    .build();
        } else {
            return MigratorProto.Import.Progress.newBuilder()
                    .setInitialising(true)
                    .setAttributesCurrent(status.attributeCount.get())
                    .build();
        }
    }

    private class ParallelImport {

        private final Function<BlockingQueue<DataProto.Item>, ImportWorker> workerConstructor;

        ParallelImport(Function<BlockingQueue<DataProto.Item>, ImportWorker> workerConstructor) {
            this.workerConstructor = workerConstructor;
        }

        void execute() {
            BlockingQueue<DataProto.Item> items = asyncItemReader();
            CompletableFuture<Void>[] workers = new CompletableFuture[parallelisation];
            for (int i = 0; i < parallelisation; i++) {
                workers[i] = CompletableFuture.runAsync(() -> workerConstructor.apply(items).run(), importExecutor);
            }
            CompletableFuture.allOf(workers).join();
        }

        private BlockingQueue<DataProto.Item> asyncItemReader() {
            BlockingQueue<DataProto.Item> queue = new ArrayBlockingQueue<>(1000);
            CompletableFuture.runAsync(() -> {
                try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(dataFile))) {
                    DataProto.Item item;
                    while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                        queue.put(item);
                    }
                } catch (IOException | InterruptedException e) {
                    throw TypeDBException.of(e);
                }
            }, readerExecutor);
            return queue;
        }
    }

    private abstract class ImportWorker {

        private final BlockingQueue<DataProto.Item> items;
        private final Map<ByteArray, String> bufferedToOriginalIds;
        private final Map<String, ByteArray> originalToBufferedIds;
        TypeDB.Transaction transaction;

        ImportWorker(BlockingQueue<DataProto.Item> items) {
            this.items = items;
            originalToBufferedIds = new HashMap<>();
            bufferedToOriginalIds = new HashMap<>();
            transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        }

        abstract int importItem(DataProto.Item item);

        void run() {
            int count = 0;
            DataProto.Item item;
            try {
                while ((item = items.poll(1, TimeUnit.SECONDS)) != null) {
                    if (count >= BATCH_SIZE) {
                        commitBatch();
                        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
                        count = 0;
                    }
                    count += importItem(item);
                }
                commitBatch();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }

        private void commitBatch() {
            transaction.commit();
            transaction.committedIIDs().forEachRemaining(pair ->
                    idMapper.put(bufferedToOriginalIds.get(pair.first()), pair.second())
            );
            bufferedToOriginalIds.clear();
            originalToBufferedIds.clear();
        }

        void recordCreated(ByteArray newIID, String originalID) {
            assert !originalToBufferedIds.containsKey(originalID) && !idMapper.contains(originalID);
            bufferedToOriginalIds.put(newIID, originalID);
            originalToBufferedIds.put(originalID, newIID);
        }

        protected void recordAttributeCreated(ByteArray iid, String originalID) {
            idMapper.put(originalID, iid);
        }

        Thing getImportedThing(String originalID) {
            ByteArray newIID;
            if ((newIID = originalToBufferedIds.get(originalID)) == null && (newIID = idMapper.get(originalID)) == null) {
                throw TypeDBException.of(ILLEGAL_STATE);
            } else {
                Thing thing = transaction.concepts().getThing(newIID);
                assert thing != null;
                return thing;
            }
        }

        boolean isImported(String originalID) {
            return originalToBufferedIds.containsKey(originalID) || idMapper.contains(originalID);
        }

        int insertOwnerships(String originalId, List<DataProto.Item.OwnedAttribute> ownerships) {
            Thing owner = getImportedThing(originalId);
            int count = 0;
            for (DataProto.Item.OwnedAttribute ownership : ownerships) {
                Thing attrThing = getImportedThing(ownership.getId());
                assert attrThing != null;
                owner.setHas(attrThing.asAttribute());
                count++;
            }
            status.ownershipCount.addAndGet(count);
            return count;
        }
    }

    private class AttributesAndChecksum extends ImportWorker {

        AttributesAndChecksum(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        int importItem(DataProto.Item item) {
            switch (item.getItemCase()) {
                case ATTRIBUTE:
                    insertAttribute(transaction, item.getAttribute());
                    return 1;
                case CHECKSUMS:
                    DataProto.Item.Checksums checksums = item.getChecksums();
                    DataImporter.this.checksum = new Checksum(checksums.getAttributeCount(), checksums.getEntityCount(),
                            checksums.getRelationCount(), checksums.getOwnershipCount(), checksums.getRoleCount());
                    return 0;
                default:
                    return 0;
            }
        }

        private void insertAttribute(TypeDB.Transaction transaction, DataProto.Item.Attribute attrMsg) {
            AttributeType type = transaction.concepts().getAttributeType(attrMsg.getLabel());
            if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, attrMsg.getLabel());
            DataProto.ValueObject valueMsg = attrMsg.getValue();
            Attribute attribute;
            switch (valueMsg.getValueCase()) {
                case STRING:
                    attribute = type.asString().put(valueMsg.getString());
                    break;
                case BOOLEAN:
                    attribute = type.asBoolean().put(valueMsg.getBoolean());
                    break;
                case LONG:
                    attribute = type.asLong().put(valueMsg.getLong());
                    break;
                case DOUBLE:
                    attribute = type.asDouble().put(valueMsg.getDouble());
                    break;
                case DATETIME:
                    attribute = type.asDateTime().put(
                            Instant.ofEpochMilli(valueMsg.getDatetime()).atZone(ZoneId.of("Z")).toLocalDateTime());
                    break;
                default:
                    throw TypeDBException.of(INVALID_DATA);
            }
            recordAttributeCreated(attribute.getIID(), attrMsg.getId());
            status.attributeCount.incrementAndGet();
        }
    }

    private class EntitiesAndOwnerships extends ImportWorker {

        EntitiesAndOwnerships(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        int importItem(DataProto.Item item) {
            switch (item.getItemCase()) {
                case ENTITY:
                    insertEntity(transaction, item.getEntity());
                    return 1 + insertOwnerships(item.getEntity().getId(), item.getEntity().getAttributeList());
                case ATTRIBUTE:
                    return insertOwnerships(item.getAttribute().getId(), item.getAttribute().getAttributeList());
                default:
                    return 0;
            }
        }

        private void insertEntity(TypeDB.Transaction transaction, DataProto.Item.Entity msg) {
            EntityType type = transaction.concepts().getEntityType(msg.getLabel());
            if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, msg.getLabel());
            Entity entity = type.create();
            recordCreated(entity.getIID(), msg.getId());
            status.entityCount.incrementAndGet();
        }
    }

    private class CompleteRelations extends ImportWorker {

        CompleteRelations(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        int importItem(DataProto.Item item) {
            if (item.getItemCase() == DataProto.Item.ItemCase.RELATION) {
                Optional<Integer> inserted = tryInsertCompleteRelation(item.getRelation());
                if (inserted.isPresent()) {
                    return inserted.get() + insertOwnerships(item.getRelation().getId(), item.getRelation().getAttributeList());
                } else {
                    skippedRelations.add(item.getRelation());
                    return 0;
                }
            } else return 0;
        }

        private Optional<Integer> tryInsertCompleteRelation(DataProto.Item.Relation relationMsg) {
            RelationType relationType = transaction.concepts().getRelationType(relationMsg.getLabel());
            Optional<List<Pair<RoleType, Thing>>> players;
            if (relationType == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, relationMsg.getLabel());
            } else if ((players = getAllPlayers(relationType, relationMsg)).isPresent()) {
                assert players.get().size() > 0;
                Relation relation = relationType.create();
                recordCreated(relation.getIID(), relationMsg.getId());
                players.get().forEach(rp -> relation.addPlayer(rp.first(), rp.second()));
                status.relationCount.incrementAndGet();
                status.roleCount.addAndGet(players.get().size());
                return Optional.of(1 + players.get().size());
            } else {
                return Optional.empty();
            }
        }

        private Optional<List<Pair<RoleType, Thing>>> getAllPlayers(RelationType type, DataProto.Item.Relation msg) {
            if (msg.getRoleList().size() == 0) throw TypeDBException.of(NO_PLAYERS, msg.getId(), type.getLabel());
            List<Pair<RoleType, Thing>> players = new ArrayList<>();
            for (DataProto.Item.Relation.Role roleMsg : msg.getRoleList()) {
                RoleType roleType = getRoleType(type, roleMsg);
                for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                    if (!isImported(playerMessage.getId())) return Optional.empty();
                    else players.add(new Pair<>(roleType, getImportedThing(playerMessage.getId())));
                }
            }
            return Optional.of(players);
        }
    }

    private void importSkippedRelations() {
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            skippedRelations.forEach(msg -> {
                RelationType relType = transaction.concepts().getRelationType(msg.getLabel());
                if (relType == null) throw TypeDBException.of(TYPE_NOT_FOUND, msg.getLabel());
                Relation importedRelation = relType.create();
                idMapper.put(msg.getId(), importedRelation.getIID());

                int ownershipCount = 0;
                for (DataProto.Item.OwnedAttribute ownership : msg.getAttributeList()) {
                    Thing attribute = transaction.concepts().getThing(idMapper.get(ownership.getId()));
                    assert attribute != null;
                    importedRelation.setHas(attribute.asAttribute());
                    ownershipCount++;
                }
                status.ownershipCount.addAndGet(ownershipCount);
            });

            skippedRelations.forEach(msg -> {
                RelationType relType = transaction.concepts().getRelationType(msg.getLabel());
                Relation relation = transaction.concepts().getThing(idMapper.get(msg.getId())).asRelation();
                msg.getRoleList().forEach(roleMsg -> {
                    RoleType roleType = getRoleType(relType, roleMsg);
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = transaction.concepts().getThing(idMapper.get(playerMessage.getId()));
                        if (player == null) throw TypeDBException.of(PLAYER_NOT_FOUND, relType.getLabel());
                        else relation.addPlayer(roleType, player);
                    }
                });
            });
            transaction.commit();
        }
    }

    private RoleType getRoleType(RelationType relationType, DataProto.Item.Relation.Role roleMsg) {
        String unscopedRoleLabel;
        String roleLabel = roleMsg.getLabel();
        if (roleLabel.contains(":")) unscopedRoleLabel = roleLabel.split(":")[1];
        else unscopedRoleLabel = roleLabel;
        RoleType roleType = relationType.getRelates(unscopedRoleLabel);
        if (roleType == null) {
            throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, roleLabel, relationType.getLabel().name());
        } else return roleType;
    }

    private static class IDMapper {

        private static final String DIRECTORY_PREFIX = "typedb-import-files-";

        private final RocksDB storage;
        private final Path directory;

        IDMapper(String database) {
            try {
                directory = Files.createTempDirectory(DIRECTORY_PREFIX + database);
                LOG.info("Import started with '" + directory + "' for auxiliary files.");
                assert !Files.list(directory).findFirst().isPresent();
                storage = RocksDB.open(directory.toString());
            } catch (IOException | RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public void close() {
            storage.close();
            cleanupDirectory();
        }

        private void cleanupDirectory() {
            try {
                Files.walk(directory).sorted(reverseOrder()).map(Path::toFile).forEach(path -> {
                    boolean deleted = path.delete();
                    if (!deleted) LOG.warn("Failed to delete temporary file '" + path.toString() + "'");
                });
            } catch (IOException e) {
                throw TypeDBException.of(e);
            }
        }

        public void put(String originalID, ByteArray newID) {
            ByteArray original = ByteArray.encodeString(originalID, STRING_ENCODING);
            try {
                storage.put(original.getBytes(), newID.getBytes());
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public ByteArray get(String originalID) {
            try {
                byte[] value = storage.get(ByteArray.encodeString(originalID, STRING_ENCODING).getBytes());
                assert value == null || value.length > 0;
                if (value == null) return null;
                else return ByteArray.of(value);
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public boolean contains(String originalID) {
            return get(originalID) != null;
        }
    }

    private static class Status {

        private final AtomicLong entityCount = new AtomicLong(0);
        private final AtomicLong relationCount = new AtomicLong(0);
        private final AtomicLong attributeCount = new AtomicLong(0);
        private final AtomicLong ownershipCount = new AtomicLong(0);
        private final AtomicLong roleCount = new AtomicLong(0);

        @Override
        public String toString() {
            return String.format("%d entities, %d attributes (%d ownerships), %d relations (%d roles)",
                    entityCount.get(), attributeCount.get(), ownershipCount.get(), relationCount.get(), roleCount.get());
        }
    }

    private static class Checksum {

        private final long attributes;
        private final long entities;
        private final long relations;
        private final long ownerships;
        private final long roles;

        Checksum(long attributes, long entities, long relations, long ownerships, long roles) {
            this.attributes = attributes;
            this.entities = entities;
            this.relations = relations;
            this.ownerships = ownerships;
            this.roles = roles;
        }

        public boolean verify(Status status) {
            return attributes == status.attributeCount.get() && entities == status.entityCount.get() &&
                    relations == status.relationCount.get() && ownerships == status.ownershipCount.get() &&
                    roles == status.roleCount.get();
        }

        public String mismatch(Status status) {
            assert !verify(status);
            String mismatch = "";
            if (attributes != status.attributeCount.get()) {
                mismatch += "\nAttribute count mismatch: expected" + attributes + ", but imported " + status.attributeCount.get();
            }
            if (entities != status.entityCount.get()) {
                mismatch += "\nEntity count mismatch: expected" + entities + ", but imported " + status.entityCount.get();
            }
            if (relations != status.relationCount.get()) {
                mismatch += "\nRelation count mismatch: expected" + relations + ", but imported " + status.relationCount.get();
            }
            if (roles != status.roleCount.get()) {
                mismatch += "\nRole count mismatch: expected" + roles + ", but imported " + status.roleCount.get();
            }
            if (ownerships != status.ownershipCount.get()) {
                mismatch += "\nOwnership count mismatch: expected" + ownerships + ", but imported " + status.ownershipCount.get();
            }
            return mismatch;
        }
    }
}
