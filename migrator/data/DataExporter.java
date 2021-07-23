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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.migrator.MigratorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_WRITABLE;
import static java.nio.charset.StandardCharsets.UTF_16;

public class DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(DataExporter.class);
    private static final Charset BYTES_ENCODING = UTF_16;

    private final TypeDB typedb;
    private final String database;
    private final Path filename;
    private final String version;
    private final Status status;
    private long totalEntityCount;
    private long totalAttributeCount;
    private long totalRelationCount;

    public DataExporter(TypeDB typedb, String database, Path filename, String version) {
        if (!typedb.databases().contains(database)) throw TypeDBException.of(DATABASE_NOT_FOUND, database);
        this.typedb = typedb;
        this.database = database;
        this.filename = filename;
        this.version = version;
        this.status = new Status();
    }

    public void run() {
        LOG.info("Exporting {} from TypeDB {}", database, version);
        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(filename))) {
            export(outputStream, header());
            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA);
                 TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                totalEntityCount = tx.concepts().getRootEntityType().getInstancesCount();
                totalAttributeCount = tx.concepts().getRootAttributeType().getInstancesCount();
                totalRelationCount = tx.concepts().getRootRelationType().getInstancesCount();

                List<Runnable> exporters = Arrays.asList(() -> exportEntities(tx, outputStream),
                        () -> exportRelations(tx, outputStream), () -> exportAttributes(tx, outputStream));
                exporters.parallelStream().forEach(Runnable::run);
                export(outputStream, checksums());
            }
        } catch (IOException e) {
            throw TypeDBException.of(FILE_NOT_WRITABLE, filename.toString());
        }
        LOG.info("Exported " + status.toString());
    }

    public MigratorProto.Export.Progress getProgress() {
        return MigratorProto.Export.Progress.newBuilder()
                .setAttributesCurrent(status.attributeCount.get())
                .setEntitiesCurrent(status.entityCount.get())
                .setRelationsCurrent(status.relationCount.get())
                .setAttributes(totalAttributeCount)
                .setEntities(totalEntityCount)
                .setRelations(totalRelationCount)
                .build();
    }

    private void exportEntities(TypeDB.Transaction tx, OutputStream out) {
        tx.concepts().getRootEntityType().getInstances().forEachRemaining(entity -> export(out, entity(entity)));
    }

    private void exportAttributes(TypeDB.Transaction tx, OutputStream out) {
        tx.concepts().getRootAttributeType().getInstances().forEachRemaining(attribute -> export(out, attribute(attribute)));
    }

    private void exportRelations(TypeDB.Transaction tx, OutputStream out) {
        tx.concepts().getRootRelationType().getInstances().forEachRemaining(relation -> export(out, relation(relation)));
    }

    private DataProto.Item header() {
        return DataProto.Item.newBuilder().setHeader(
                DataProto.Item.Header.newBuilder()
                        .setOriginalDatabase(database)
                        .setTypedbVersion(version)
                        .build()
        ).build();
    }

    private DataProto.Item entity(Entity entity) {
        status.entityCount.incrementAndGet();
        DataProto.Item.Entity.Builder entityBuilder = DataProto.Item.Entity.newBuilder()
                .setId(entity.getIID().decodeString(BYTES_ENCODING))
                .setLabel(entity.getType().getLabel().name());
        readOwnerships(entity).forEachRemaining(a -> {
            status.ownershipCount.incrementAndGet();
            entityBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setEntity(entityBuilder).build();
    }

    private DataProto.Item relation(Relation relation) {
        status.relationCount.incrementAndGet();
        DataProto.Item.Relation.Builder relationBuilder = DataProto.Item.Relation.newBuilder()
                .setId(relation.getIID().decodeString(BYTES_ENCODING))
                .setLabel(relation.getType().getLabel().name());
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> rolePlayers : playersByRole.entrySet()) {
            RoleType role = rolePlayers.getKey();
            DataProto.Item.Relation.Role.Builder roleBuilder = DataProto.Item.Relation.Role.newBuilder()
                    .setLabel(role.getLabel().scopedName());
            for (Thing player : rolePlayers.getValue()) {
                status.roleCount.incrementAndGet();
                roleBuilder.addPlayer(DataProto.Item.Relation.Role.Player.newBuilder()
                        .setId(player.getIID().decodeString(BYTES_ENCODING)));
            }
            relationBuilder.addRole(roleBuilder);
        }
        readOwnerships(relation).forEachRemaining(a -> {
            status.ownershipCount.incrementAndGet();
            relationBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setRelation(relationBuilder).build();
    }

    private DataProto.Item attribute(Attribute attribute) {
        status.attributeCount.incrementAndGet();
        DataProto.Item.Attribute.Builder attributeBuilder = DataProto.Item.Attribute.newBuilder()
                .setId(attribute.getIID().decodeString(BYTES_ENCODING))
                .setLabel(attribute.getType().getLabel().name())
                .setValue(value(attribute));
        readOwnerships(attribute).forEachRemaining(a -> {
            status.ownershipCount.incrementAndGet();
            attributeBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setAttribute(attributeBuilder).build();
    }

    private DataProto.ValueObject.Builder value(Attribute attribute) {
        DataProto.ValueObject.Builder valueObject = DataProto.ValueObject.newBuilder();
        if (attribute.isString()) {
            valueObject.setString(attribute.asString().getValue());
        } else if (attribute.isBoolean()) {
            valueObject.setBoolean(attribute.asBoolean().getValue());
        } else if (attribute.isLong()) {
            valueObject.setLong(attribute.asLong().getValue());
        } else if (attribute.isDouble()) {
            valueObject.setDouble(attribute.asDouble().getValue());
        } else if (attribute.isDateTime()) {
            valueObject.setDatetime(attribute.asDateTime().getValue().atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
        return valueObject;
    }

    private FunctionalIterator<DataProto.Item.OwnedAttribute.Builder> readOwnerships(Thing thing) {
        return thing.getHas().map(attribute -> DataProto.Item.OwnedAttribute.newBuilder()
                .setId(attribute.getIID().decodeString(BYTES_ENCODING)));
    }

    private static class Status {

        private final AtomicLong entityCount = new AtomicLong(0);
        private final AtomicLong attributeCount = new AtomicLong(0);
        private final AtomicLong ownershipCount = new AtomicLong(0);
        private final AtomicLong relationCount = new AtomicLong(0);
        private final AtomicLong roleCount = new AtomicLong(0);

        @Override
        public String toString() {
            return String.format("%d entities, %d attributes (%d ownerships), %d relations (%d roles)",
                    entityCount.get(), attributeCount.get(), ownershipCount.get(), relationCount.get(), roleCount.get());
        }
    }

    private DataProto.Item checksums() {
        return DataProto.Item.newBuilder().setChecksums(
                DataProto.Item.Checksums.newBuilder()
                        .setEntityCount(status.entityCount.get())
                        .setAttributeCount(status.attributeCount.get())
                        .setRelationCount(status.relationCount.get())
                        .setRoleCount(status.roleCount.get())
                        .setOwnershipCount(status.ownershipCount.get())
        ).build();
    }

    private synchronized void export(OutputStream outputStream, DataProto.Item item) {
        try {
            item.writeDelimitedTo(outputStream);
        } catch (IOException e) {
            throw TypeDBException.of(FILE_NOT_WRITABLE, filename.toString());
        }
    }
}
