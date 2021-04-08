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

package grakn.core.migrator;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RoleType;
import grakn.core.migrator.proto.DataProto;
import grakn.core.migrator.proto.MigratorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Migrator.FILE_NOT_WRITABLE;

public class DataExporter implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(DataExporter.class);
    private final Grakn grakn;
    private final String database;
    private final Path filename;
    private final String version;
    private final AtomicLong entityCount = new AtomicLong(0);
    private final AtomicLong relationCount = new AtomicLong(0);
    private final AtomicLong attributeCount = new AtomicLong(0);
    private final AtomicLong ownershipCount = new AtomicLong(0);
    private final AtomicLong playerCount = new AtomicLong(0);
    private long totalThingCount = 0;

    DataExporter(Grakn grakn, String database, Path filename, String version) {
        this.grakn = grakn;
        this.database = database;
        this.filename = filename;
        this.version = version;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        long current = attributeCount.get() + relationCount.get() + entityCount.get();
        return MigratorProto.Job.Progress.newBuilder()
                .setCurrent(current)
                .setTotal(totalThingCount)
                .build();
    }

    @Override
    public void run() {
        LOG.info("Exporting {} from Grakn {}", database, version);
        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(filename))) {
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA);
                 Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                totalThingCount = tx.concepts().getRootThingType().getInstancesCount();
                DataProto.Item header = DataProto.Item.newBuilder().setHeader(
                        DataProto.Item.Header.newBuilder()
                                .setGraknVersion(version)
                                .setOriginalDatabase(session.database().name())
                ).build();
                write(outputStream, header);

                List<Runnable> workers = new ArrayList<>();
                workers.add(() -> tx.concepts().getRootEntityType().getInstances().forEachRemaining(entity -> {
                    DataProto.Item item = readEntity(entity);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootRelationType().getInstances().forEachRemaining(relation -> {
                    DataProto.Item item = readRelation(relation);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootAttributeType().getInstances().forEachRemaining(attribute -> {
                    DataProto.Item item = readAttribute(attribute);
                    write(outputStream, item);
                }));
                workers.parallelStream().forEach(Runnable::run);

                DataProto.Item checksums = DataProto.Item.newBuilder().setChecksums(
                        DataProto.Item.Checksums.newBuilder()
                                .setEntityCount(entityCount.get())
                                .setAttributeCount(attributeCount.get())
                                .setRelationCount(relationCount.get())
                                .setRoleCount(playerCount.get())
                                .setOwnershipCount(ownershipCount.get())
                ).build();
                write(outputStream, checksums);
            }
        } catch (IOException e) {
            throw GraknException.of(FILE_NOT_WRITABLE, filename.toString());
        }
        LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                 entityCount.get(),
                 attributeCount.get(),
                 relationCount.get(),
                 playerCount.get(),
                 ownershipCount.get());
    }

    private DataProto.Item readEntity(Entity entity) {
        entityCount.incrementAndGet();
        DataProto.Item.Entity.Builder entityBuilder = DataProto.Item.Entity.newBuilder()
                .setId(new String(entity.getIID()))
                .setLabel(entity.getType().getLabel().name());
        readOwnerships(entity).forEachRemaining(a -> {
            ownershipCount.incrementAndGet();
            entityBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setEntity(entityBuilder).build();
    }

    private DataProto.Item readRelation(Relation relation) {
        relationCount.incrementAndGet();
        DataProto.Item.Relation.Builder relationBuilder = DataProto.Item.Relation.newBuilder()
                .setId(new String(relation.getIID()))
                .setLabel(relation.getType().getLabel().name());
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> rolePlayers : playersByRole.entrySet()) {
            RoleType role = rolePlayers.getKey();
            DataProto.Item.Relation.Role.Builder roleBuilder = DataProto.Item.Relation.Role.newBuilder()
                    .setLabel(role.getLabel().scopedName());
            for (Thing player : rolePlayers.getValue()) {
                playerCount.incrementAndGet();
                roleBuilder.addPlayer(DataProto.Item.Relation.Role.Player.newBuilder()
                                              .setId(new String(player.getIID())));
            }
            relationBuilder.addRole(roleBuilder);
        }
        readOwnerships(relation).forEachRemaining(a -> {
            ownershipCount.incrementAndGet();
            relationBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setRelation(relationBuilder).build();
    }

    private DataProto.Item readAttribute(Attribute attribute) {
        attributeCount.incrementAndGet();
        DataProto.Item.Attribute.Builder attributeBuilder = DataProto.Item.Attribute.newBuilder()
                .setId(new String(attribute.getIID()))
                .setLabel(attribute.getType().getLabel().name())
                .setValue(readValue(attribute));
        readOwnerships(attribute).forEachRemaining(a -> {
            ownershipCount.incrementAndGet();
            attributeBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setAttribute(attributeBuilder).build();
    }

    private DataProto.ValueObject.Builder readValue(Attribute attribute) {
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
            throw GraknException.of(ILLEGAL_STATE);
        }
        return valueObject;
    }

    private FunctionalIterator<DataProto.Item.OwnedAttribute.Builder> readOwnerships(Thing thing) {
        return thing.getHas().map(attribute -> DataProto.Item.OwnedAttribute.newBuilder()
                .setId(new String(attribute.getIID())));
    }

    private synchronized void write(OutputStream outputStream, DataProto.Item item) {
        try {
            item.writeDelimitedTo(outputStream);
        } catch (IOException e) {
            throw GraknException.of(FILE_NOT_WRITABLE, filename.toString());
        }
    }
}
