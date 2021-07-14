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
import com.vaticle.typedb.core.migrator.data.DataProto;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.PLAYER_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.migrator.data.DataProto.Item.ItemCase.HEADER;
import static java.util.Comparator.reverseOrder;

public class DataImporter {

    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 1000;
    private final RocksSession session;
    private final Executor importExecutor;
    private final Executor readerExecutor;
    private final int parallelism;
    private final Path dataFile;

    private final Map<String, String> remapLabels;
    private final IDMapper idMapper;
    private final String version;
    private final Status status;
    private final ConcurrentSet<DataProto.Item.Relation> skippedRelations;
    private Checksum checksum;

    // TODO should not use impl but interface instead
    public DataImporter(RocksTypeDB typedb, String database, Path dataFile, Map<String, String> remapLabels, String version) {
        if (!Files.exists(dataFile)) throw TypeDBException.of(FILE_NOT_FOUND, dataFile);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.dataFile = dataFile;
        this.remapLabels = remapLabels;
        this.version = version;
        assert com.vaticle.typedb.core.concurrent.executor.Executors.isInitialised();
        // TODO call this a more consistent name
        this.parallelism = com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
        this.importExecutor = Executors.newFixedThreadPool(parallelism);
        this.readerExecutor = Executors.newSingleThreadExecutor();
        this.idMapper = new IDMapper(database);
        this.skippedRelations = new ConcurrentSet<>();
        this.status = new Status();
    }

    public void run() {
        try {
            readHeader();
            new ParallelImport(AttributesAndChecksum::new).run();
            new ParallelImport(EntitiesAndOwnerships::new).run();
            new ParallelImport(CompleteRelations::new).run();
            importSkippedRelations();
            if (!checksum.verify(status)) throw TypeDBException.of(IMPORT_CHECKSUM_MISMATCH);
        } finally {
            LOG.info("Imported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                    // TODO status should print itself as "Import status: " + status.toString()
                    status.entityCount.get(),
                    status.attributeCount.get(),
                    status.relationCount.get(),
                    status.roleCount.get(),
                    status.ownershipCount.get());
        }
    }

    public void close() {
        session.close();
        idMapper.close();
    }

    // TODO call this validateHeader() and
    // TODO and throw a more contextualised error
    private void readHeader() {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(dataFile))) {
            DataProto.Item item = ITEM_PARSER.parseDelimitedFrom(inputStream);
            assert item.getItemCase().equals(HEADER);
            DataProto.Item.Header header = item.getHeader();
            LOG.info("Importing {} from TypeDB {} to {} in TypeDB {}",
                    header.getOriginalDatabase(),
                    header.getTypedbVersion(),
                    session.database().name(),
                    version);
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

        void run() {
            BlockingQueue<DataProto.Item> items = readDataFile();
            CompletableFuture<Void>[] workers = new CompletableFuture[parallelism];
            for (int i = 0; i < parallelism; i++) {
                workers[i] = CompletableFuture.runAsync(() -> workerConstructor.apply(items).run(), importExecutor);
            }
            CompletableFuture.allOf(workers).join();
        }

        private BlockingQueue<DataProto.Item> readDataFile() {
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

        private final Map<ByteArray, String> bufferedIIDsToOriginalIds;
        private final Map<String, ByteArray> originalIDsToBufferedIIDs;
        private final BlockingQueue<DataProto.Item> items;
        RocksTransaction transaction;

        ImportWorker(BlockingQueue<DataProto.Item> items) {
            this.items = items;
            transaction = session.transaction(Arguments.Transaction.Type.WRITE);
            originalIDsToBufferedIIDs = new HashMap<>();
            bufferedIIDsToOriginalIds = new HashMap<>();
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

        void recordCreated(ByteArray newIID, String originalID) {
            assert !originalIDsToBufferedIIDs.containsKey(originalID) && !idMapper.contains(originalID);
            bufferedIIDsToOriginalIds.put(newIID, originalID);
            originalIDsToBufferedIIDs.put(originalID, newIID);
        }

        protected void recordAttributeCreated(ByteArray iid, String originalID) {
            idMapper.put(originalID, iid);
        }

        Thing getImportedThing(String originalID) {
            ByteArray newIID;
            if ((newIID = originalIDsToBufferedIIDs.get(originalID)) == null && (newIID = idMapper.get(originalID)) == null) {
                throw TypeDBException.of(ILLEGAL_STATE);
            } else {
                Thing thing = transaction.concepts().getThing(newIID);
                assert thing != null;
                return thing;
            }
        }

        boolean isImported(String originalID) {
            return originalIDsToBufferedIIDs.containsKey(originalID) || idMapper.contains(originalID);
        }

        int insertOwnerships(String originalId, List<DataProto.Item.OwnedAttribute> ownedMsgs) {
            Thing owner = getImportedThing(originalId);
            int ownerships = 0;
            for (DataProto.Item.OwnedAttribute ownedMsg : ownedMsgs) {
                Thing attrThing = getImportedThing(ownedMsg.getId());
                assert attrThing != null;
                owner.setHas(attrThing.asAttribute());
                ownerships++;
            }
            status.ownershipCount.addAndGet(ownerships);
            return ownerships;
        }

        private void commitBatch() {
            transaction.commit();
            // TODO use .asData() to do the cast
            ((RocksTransaction.Data) transaction).committedIIDs().forEachRemaining(pair -> {
                idMapper.put(bufferedIIDsToOriginalIds.get(pair.first()), pair.second());
            });
            bufferedIIDsToOriginalIds.clear();
            originalIDsToBufferedIIDs.clear();
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

        private void insertAttribute(TypeDB.Transaction transaction, DataProto.Item.Attribute attributeMsg) {
            AttributeType attributeType = transaction.concepts().getAttributeType(relabel(attributeMsg.getLabel()));
            if (attributeType != null) {
                DataProto.ValueObject valueMsg = attributeMsg.getValue();
                Attribute attribute;
                switch (valueMsg.getValueCase()) {
                    case STRING:
                        attribute = attributeType.asString().put(valueMsg.getString());
                        break;
                    case BOOLEAN:
                        attribute = attributeType.asBoolean().put(valueMsg.getBoolean());
                        break;
                    case LONG:
                        attribute = attributeType.asLong().put(valueMsg.getLong());
                        break;
                    case DOUBLE:
                        attribute = attributeType.asDouble().put(valueMsg.getDouble());
                        break;
                    case DATETIME:
                        attribute = attributeType.asDateTime().put(
                                Instant.ofEpochMilli(valueMsg.getDatetime()).atZone(ZoneId.of("Z")).toLocalDateTime());
                        break;
                    default:
                        throw TypeDBException.of(INVALID_DATA);
                }
                recordAttributeCreated(attribute.getIID(), attributeMsg.getId());
                status.attributeCount.incrementAndGet();
            } else {
                throw TypeDBException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
            }
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

        private void insertEntity(TypeDB.Transaction transaction, DataProto.Item.Entity entityMsg) {
            EntityType entityType = transaction.concepts().getEntityType(relabel(entityMsg.getLabel()));
            if (entityType != null) {
                Entity entity = entityType.create();
                recordCreated(entity.getIID(), entityMsg.getId());
                status.entityCount.incrementAndGet();
            } else {
                throw TypeDBException.of(TYPE_NOT_FOUND, relabel(entityMsg.getLabel()), entityMsg.getLabel());
            }
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
            RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
            Optional<List<Pair<RoleType, Thing>>> players;
            if (relationType == null) {
                throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
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

        private Optional<List<Pair<RoleType, Thing>>> getAllPlayers(RelationType relationType, DataProto.Item.Relation relationMsg) {
            assert relationMsg.getRoleList().size() > 0;
            List<Pair<RoleType, Thing>> players = new ArrayList<>();
            for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                RoleType roleType = getRoleType(relationType, roleMsg);
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
            skippedRelations.forEach(relationMsg -> {
                RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
                if (relationType == null) {
                    throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
                }
                Relation relation = relationType.create();
                idMapper.put(relationMsg.getId(), relation.getIID());
            });

            skippedRelations.forEach(relationMsg -> {
                RelationType relationType = transaction.concepts().getRelationType(relabel(relationMsg.getLabel()));
                Relation relation = transaction.concepts().getThing(idMapper.get(relationMsg.getId())).asRelation();
                relationMsg.getRoleList().forEach(roleMsg -> {
                    RoleType roleType = getRoleType(relationType, roleMsg);
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = transaction.concepts().getThing(idMapper.get(playerMessage.getId()));
                        if (player == null) throw TypeDBException.of(PLAYER_NOT_FOUND, relationType.getLabel());
                        else relation.addPlayer(roleType, player);
                    }
                });
            });
            transaction.commit();
        }
    }

    private RoleType getRoleType(RelationType relationType, DataProto.Item.Relation.Role roleMsg) {
        String unscopedRoleLabel;
        String roleLabel = relabel(roleMsg.getLabel());
        if (roleLabel.contains(":")) unscopedRoleLabel = roleLabel.split(":")[1];
        else unscopedRoleLabel = roleLabel;
        RoleType roleType = relationType.getRelates(unscopedRoleLabel);
        if (roleType == null) {
            throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, roleLabel, roleMsg.getLabel(), relationType.getLabel().name());
        } else return roleType;
    }

    private String relabel(String label) {
        return remapLabels.getOrDefault(label, label);
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
            ByteArray original = ByteArray.encodeString(originalID);
            try {
                storage.put(original.getBytes(), newID.getBytes());
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public ByteArray get(String originalID) {
            try {
                byte[] value = storage.get(ByteArray.encodeString(originalID).getBytes());
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
            return attributes == status.attributeCount.get() &&
                    entities == status.entityCount.get() &&
                    relations == status.relationCount.get() &&
                    ownerships == status.ownershipCount.get() &&
                    roles == status.roleCount.get();
        }
    }
}
