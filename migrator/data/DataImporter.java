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
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.collection.Bytes.unsignedByte;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.IMPORT_CHECKSUM_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.MISSING_HEADER;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.PLAYER_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.migrator.data.DataProto.Item.ItemCase.HEADER;
import static java.util.Comparator.reverseOrder;

public class DataImporter {
    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);

    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 1000;
    private final TypeDB.Session session;
    private final ExecutorService importExecutor;
    private final ExecutorService readerExecutor;
    private final int parallelisation;

    private final Path dataFile;
    private final ConceptTracker conceptTracker;
    private final String version;
    private final Status status;
    private final AtomicBoolean skippedRelations;
    private Checksum checksum;

    public DataImporter(TypeDB.DatabaseManager typedb, String database, Path dataFile, String version) {
        if (!Files.exists(dataFile)) throw TypeDBException.of(FILE_NOT_FOUND, dataFile);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.dataFile = dataFile;
        this.version = version;
        assert com.vaticle.typedb.core.concurrent.executor.Executors.isInitialised();
        this.parallelisation = com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
        this.importExecutor = Executors.newFixedThreadPool(parallelisation * 2);
        this.readerExecutor = Executors.newSingleThreadExecutor();
        this.conceptTracker = new ConceptTracker(database);
        this.skippedRelations = new AtomicBoolean(false);
        this.status = new Status();
    }

    public void run() {
        try {
            Instant start = Instant.now();
            validateHeader();
            new ParallelImport(AttributesAndChecksum::new).executeImport();
            new ParallelImport(EntitiesAndOwnerships::new).executeImport();
            importRelations();
            if (!checksum.verify(status)) throw TypeDBException.of(IMPORT_CHECKSUM_MISMATCH, checksum.mismatch(status));
            Instant end = Instant.now();
            LOG.info("Finished in: " + Duration.between(start, end).getSeconds() + " seconds");
            LOG.info("Imported: " + status.toString());
        } finally {
            importExecutor.shutdownNow();
            readerExecutor.shutdownNow();
        }
    }

    public void close() {
        session.close();
        conceptTracker.close();
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

        void executeImport() {
            BlockingQueue<DataProto.Item> items = asyncItemReader();
            CompletableFuture<Void>[] workers = new CompletableFuture[parallelisation];
            for (int i = 0; i < parallelisation; i++) {
                workers[i] = CompletableFuture.runAsync(() -> workerConstructor.apply(items).importItems(), importExecutor);
            }
            try {
                CompletableFuture.allOf(workers).join();
            } catch (CompletionException exception) {
                throw TypeDBException.of(exception);
            }
        }

        private BlockingQueue<DataProto.Item> asyncItemReader() {
            BlockingQueue<DataProto.Item> queue = new ArrayBlockingQueue<>(4000);
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
        private final Map<ByteArray, String> bufferedToOriginalIDs;
        private final Map<String, ByteArray> originalToBufferedIDs;
        private final Set<String> incompleteIDs;
        private final Set<String> completedIDs;
        TypeDB.Transaction transaction;

        ImportWorker(BlockingQueue<DataProto.Item> items) {
            this.items = items;
            originalToBufferedIDs = new HashMap<>();
            bufferedToOriginalIDs = new HashMap<>();
            incompleteIDs = new HashSet<>();
            completedIDs = new HashSet<>();
        }

        abstract long importItem(DataProto.Item item);

        void importItems() {
            int count = 0;
            DataProto.Item item;
            try {
                transaction = session.transaction(Arguments.Transaction.Type.WRITE);
                while ((item = items.poll(1, TimeUnit.SECONDS)) != null) {
                    if (count >= BATCH_SIZE) {
                        commitBatch();
                        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
                        count = 0;
                    }
                    count += importItem(item);
                }
                commitBatch();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                transaction.close();
            }
        }

        private void commitBatch() {
            assert originalToBufferedIDs.keySet().containsAll(incompleteIDs);
            transaction.commit();
            transaction.committedIIDs().forEachRemaining(pair -> {
                String originalID = bufferedToOriginalIDs.get(pair.first());
                conceptTracker.recordMapped(originalID, pair.second());
                if (incompleteIDs.contains(originalID)) conceptTracker.recordIncomplete(originalID);
            });
            bufferedToOriginalIDs.clear();
            originalToBufferedIDs.clear();
            completedIDs.forEach(conceptTracker::deleteIncomplete);
        }

        void recordMapping(ByteArray newIID, String originalID) {
            assert !originalToBufferedIDs.containsKey(originalID) && !conceptTracker.containsMapped(originalID);
            bufferedToOriginalIDs.put(newIID, originalID);
            originalToBufferedIDs.put(originalID, newIID);
        }

        protected void recordAttributeIDMapping(ByteArray IID, String originalID) {
            conceptTracker.recordMapped(originalID, IID);
        }

        void recordIncompleteID(String originalID) {
            incompleteIDs.add(originalID);
        }

        void recordCompletedID(String originalID) {
            incompleteIDs.remove(originalID);
            completedIDs.add(originalID);
        }

        boolean isIncompleteID(String id) {
            if (incompleteIDs.contains(id)) return true;
            else return conceptTracker.isIncomplete(id);
        }

        Thing getMappedThing(String originalID) {
            ByteArray newIID;
            if ((newIID = originalToBufferedIDs.get(originalID)) == null && (newIID = conceptTracker.getMapped(originalID)) == null) {
                return null;
            } else {
                Thing thing = transaction.concepts().getThing(newIID);
                assert thing != null;
                return thing;
            }
        }

        int importOwnerships(String originalID, List<DataProto.Item.OwnedAttribute> ownerships) {
            Thing owner = getMappedThing(originalID);
            int imported = 0;
            for (DataProto.Item.OwnedAttribute ownership : ownerships) {
                Thing attrThing = getMappedThing(ownership.getId());
                assert attrThing != null;
                owner.setHas(attrThing.asAttribute());
                imported++;
            }
            status.ownershipCount.addAndGet(imported);
            return imported;
        }
    }

    private class AttributesAndChecksum extends ImportWorker {

        AttributesAndChecksum(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        long importItem(DataProto.Item item) {
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
                            Instant.ofEpochMilli(valueMsg.getDatetime()).atZone(ZoneOffset.UTC).toLocalDateTime());
                    break;
                default:
                    throw TypeDBException.of(INVALID_DATA);
            }
            recordAttributeIDMapping(attribute.getIID(), attrMsg.getId());
            status.attributeCount.incrementAndGet();
        }
    }

    private class EntitiesAndOwnerships extends ImportWorker {

        EntitiesAndOwnerships(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        long importItem(DataProto.Item item) {
            switch (item.getItemCase()) {
                case ENTITY:
                    insertEntity(transaction, item.getEntity());
                    return 1 + importOwnerships(item.getEntity().getId(), item.getEntity().getAttributeList());
                case ATTRIBUTE:
                    return importOwnerships(item.getAttribute().getId(), item.getAttribute().getAttributeList());
                default:
                    return 0;
            }
        }

        private void insertEntity(TypeDB.Transaction transaction, DataProto.Item.Entity msg) {
            EntityType type = transaction.concepts().getEntityType(msg.getLabel());
            if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, msg.getLabel());
            Entity entity = type.create();
            recordMapping(entity.getIID(), msg.getId());
            status.entityCount.incrementAndGet();
        }
    }

    private class Relations extends ImportWorker {

        Relations(BlockingQueue<DataProto.Item> items) {
            super(items);
        }

        @Override
        long importItem(DataProto.Item item) {
            if (item.getItemCase() == DataProto.Item.ItemCase.RELATION) {
                Thing importedRelation = getMappedThing(item.getRelation().getId());
                if (importedRelation == null) {
                    int imported = tryImport(item.getRelation());
                    if (imported > 0) {
                        return imported + importOwnerships(item.getRelation().getId(), item.getRelation().getAttributeList());
                    }
                } else if (isIncompleteID(item.getRelation().getId())) {
                    return tryExtend(importedRelation.asRelation(), item.getRelation());
                }
            }
            return 0;
        }

        private int tryImport(DataProto.Item.Relation relationMsg) {
            RelationType relationType = transaction.concepts().getRelationType(relationMsg.getLabel());
            if (relationType == null) throw TypeDBException.of(TYPE_NOT_FOUND, relationMsg.getLabel());
            int rolesCreated = instantiate(relationType, relationMsg.getId(), relationMsg.getRoleList());
            if (rolesCreated == 0) {
                // none of the players were present, so the relation was not created
                skippedRelations.set(true);
                return 0;
            } else {
                int expectedRoles = iterate(relationMsg.getRoleList()).map(rl -> rl.getPlayerList().size()).reduce(0, Integer::sum);
                if (rolesCreated != expectedRoles) recordIncompleteID(relationMsg.getId());
                status.relationCount.incrementAndGet();
                status.roleCount.addAndGet(rolesCreated);
                return 1 + rolesCreated;
            }
        }

        private int instantiate(RelationType relationType, String originalID, List<DataProto.Item.Relation.Role> roleList) {
            int rolesCreated = 0;
            Relation relation = null;
            for (DataProto.Item.Relation.Role roleMsg : roleList) {
                RoleType roleType = getRoleType(relationType, roleMsg);
                for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                    Thing player = getMappedThing(playerMessage.getId());
                    if (player == null) continue;
                    if (relation == null) {
                        relation = relationType.create();
                        recordMapping(relation.getIID(), originalID);
                    }
                    relation.addPlayer(roleType, player);
                    rolesCreated++;
                }
            }
            return rolesCreated;
        }

        private int tryExtend(Relation relation, DataProto.Item.Relation relationMsg) {
            int rolesCreated = 0;
            boolean stillIncomplete = false;
            for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                RoleType roleType = getRoleType(relation.getType(), roleMsg);
                for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                    Thing player = getMappedThing(playerMessage.getId());
                    if (player == null) stillIncomplete = true;
                    else if (!relation.getPlayers(roleType).findFirst(player).isPresent()) {
                        relation.addPlayer(roleType, player);
                        rolesCreated++;
                    }
                }
            }
            if (!stillIncomplete) recordCompletedID(relationMsg.getId());
            status.roleCount.addAndGet(rolesCreated);
            return rolesCreated;
        }
    }

    private void importRelations() {
        boolean progressMade;
        do {
            skippedRelations.set(false);
            long before = status.relationCount.get() + status.roleCount.get();
            new ParallelImport(Relations::new).executeImport();
            progressMade = before < (status.relationCount.get() + status.roleCount.get());
        } while (relationsUnfinished() && progressMade);

        if (relationsUnfinished()) loadCyclicalRelations();
        assert !conceptTracker.containsIncomplete();
    }

    private boolean relationsUnfinished() {
        return skippedRelations.get() || conceptTracker.containsIncomplete();
    }

    private void loadCyclicalRelations() {
        // Load all relations that have only relation role players in cycles in one transaction
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            createCyclicalRelationsAndOwnerships(transaction);
            addRolePlayers(transaction);
            transaction.commit();
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }
    }

    private void createCyclicalRelationsAndOwnerships(TypeDB.Transaction transaction) throws IOException {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(dataFile))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.RELATION) {
                    Thing relation = transaction.concepts().getThing(conceptTracker.getMapped(item.getRelation().getId()));
                    if (relation != null) continue;
                    relation = transaction.concepts().getRelationType(item.getRelation().getLabel()).create();
                    status.relationCount.incrementAndGet();
                    conceptTracker.recordMapped(item.getRelation().getId(), relation.getIID());
                    conceptTracker.recordIncomplete(item.getRelation().getId());
                    for (DataProto.Item.OwnedAttribute ownership : item.getRelation().getAttributeList()) {
                        Thing attribute = transaction.concepts().getThing(conceptTracker.getMapped(ownership.getId()));
                        assert attribute != null;
                        relation.setHas(attribute.asAttribute());
                        status.ownershipCount.incrementAndGet();
                    }
                }
            }
        }
    }

    private void addRolePlayers(TypeDB.Transaction transaction) throws IOException {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(dataFile))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.RELATION && conceptTracker.isIncomplete(item.getRelation().getId())) {
                    Relation relation = transaction.concepts().getThing(conceptTracker.getMapped(item.getRelation().getId())).asRelation();
                    RelationType relationType = relation.getType();
                    item.getRelation().getRoleList().forEach(roleMsg -> {
                        RoleType roleType = getRoleType(relationType, roleMsg);
                        for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                            Thing player = transaction.concepts().getThing(conceptTracker.getMapped(playerMessage.getId()));
                            if (player == null) throw TypeDBException.of(PLAYER_NOT_FOUND, relationType.getLabel());
                            else if (!relation.asRelation().getPlayers(roleType).findFirst(player).isPresent()) {
                                relation.addPlayer(roleType, player);
                                status.roleCount.incrementAndGet();
                            }
                        }
                    });
                    conceptTracker.deleteIncomplete(item.getRelation().getId());
                }
            }
        }
    }

    private RoleType getRoleType(RelationType relationType, DataProto.Item.Relation.Role roleMsg) {
        String unscopedRoleLabel;
        String roleLabel = roleMsg.getLabel();
        if (roleLabel.contains(":")) unscopedRoleLabel = roleLabel.split(":")[1];
        else unscopedRoleLabel = roleLabel;
        RoleType roleType = relationType.getRelates(unscopedRoleLabel);
        if (roleType == null) throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, roleLabel, relationType.getLabel().name());
        else return roleType;
    }

    private static class ConceptTracker {

        private static final String DIRECTORY_PREFIX = "typedb-import-files-";

        private final RocksDB storage;
        private final Path directory;

        ConceptTracker(String database) {
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

        private void recordMapped(String originalID, ByteArray newID) {
            ByteArray originalIDEncoded = encodeOriginalID(originalID);
            ByteArray mappingKey = ByteArray.join(Prefix.ID_MAPPING.bytes, originalIDEncoded);
            try {
                storage.put(mappingKey.getBytes(), newID.getBytes());
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public ByteArray getMapped(String originalID) {
            try {
                ByteArray key = ByteArray.join(Prefix.ID_MAPPING.bytes, encodeOriginalID(originalID));
                byte[] value = storage.get(key.getBytes());
                assert value == null || value.length > 0;
                if (value == null) return null;
                else return ByteArray.of(value);
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public boolean containsMapped(String originalID) {
            return getMapped(originalID) != null;
        }

        private void recordIncomplete(String originalID) {
            ByteArray key = ByteArray.join(Prefix.ID_INCOMPLETE.bytes, encodeOriginalID(originalID));
            try {
                storage.put(key.getBytes(), new byte[0]);
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public boolean isIncomplete(String originalID) {
            ByteArray key = ByteArray.join(Prefix.ID_INCOMPLETE.bytes, encodeOriginalID(originalID));
            try {
                return storage.get(key.getBytes()) != null;
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        public boolean containsIncomplete() {
            RocksIterator iterator = storage.newIterator();
            iterator.seek(Prefix.ID_INCOMPLETE.bytes.getBytes());
            boolean hasIncomplete = iterator.isValid() && ByteArray.of(iterator.key()).hasPrefix(Prefix.ID_INCOMPLETE.bytes);
            iterator.close();
            return hasIncomplete;
        }

        private void deleteIncomplete(String originalID) {
            ByteArray key = ByteArray.join(Prefix.ID_INCOMPLETE.bytes, encodeOriginalID(originalID));
            try {
                storage.delete(key.getBytes());
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        private ByteArray encodeOriginalID(String originalID) {
            return ByteArray.encodeString(originalID, STRING_ENCODING);
        }

        enum Prefix {
            ID_MAPPING(0),
            ID_INCOMPLETE(1);
            ByteArray bytes;

            Prefix(int key) {
                this.bytes = ByteArray.of(new byte[]{unsignedByte(key)});
            }
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

        boolean verify(Status status) {
            return attributes == status.attributeCount.get() && entities == status.entityCount.get() &&
                    relations == status.relationCount.get() && ownerships == status.ownershipCount.get() &&
                    roles == status.roleCount.get();
        }

        String mismatch(Status status) {
            assert !verify(status);
            String mismatch = "";
            if (attributes != status.attributeCount.get()) {
                mismatch += "\nAttribute count mismatch: expected " + attributes + ", but imported " + status.attributeCount.get();
            }
            if (entities != status.entityCount.get()) {
                mismatch += "\nEntity count mismatch: expected " + entities + ", but imported " + status.entityCount.get();
            }
            if (relations != status.relationCount.get()) {
                mismatch += "\nRelation count mismatch: expected " + relations + ", but imported " + status.relationCount.get();
            }
            if (roles != status.roleCount.get()) {
                mismatch += "\nRole count mismatch: expected " + roles + ", but imported " + status.roleCount.get();
            }
            if (ownerships != status.ownershipCount.get()) {
                mismatch += "\nOwnership count mismatch: expected " + ownerships + ", but imported " + status.ownershipCount.get();
            }
            return mismatch;
        }
    }
}
