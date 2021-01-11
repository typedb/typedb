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

package grakn.core.server.migrator;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RoleType;
import grakn.core.server.Version;
import grakn.core.server.migrator.proto.DataProto;
import grakn.core.server.migrator.proto.MigratorProto;
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
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Migrator.FILE_NOT_WRITABLE;

public class Exporter implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(Exporter.class);
    private final Grakn grakn;
    private final String database;
    private final Path filename;
    private final AtomicLong entityCount = new AtomicLong(0);
    private final AtomicLong relationCount = new AtomicLong(0);
    private final AtomicLong attributeCount = new AtomicLong(0);
    private final AtomicLong ownershipCount = new AtomicLong(0);
    private final AtomicLong playerCount = new AtomicLong(0);
    private long totalThingCount = 0;

    public Exporter(final Grakn grakn, final String database, final Path filename) {
        this.grakn = grakn;
        this.database = database;
        this.filename = filename;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        final long current = attributeCount.get() + relationCount.get() + entityCount.get();
        return MigratorProto.Job.Progress.newBuilder()
                .setCurrent(current)
                .setTotal(totalThingCount)
                .build();
    }

    @Override
    public void run() {
        LOG.info("Exporting {} from Grakn {}", database, Version.VERSION);
        try (final OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(filename))) {
            try (final Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA);
                 final Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                totalThingCount = tx.concepts().getRootThingType().getInstancesCount();
                final DataProto.Item header = DataProto.Item.newBuilder()
                        .setHeader(DataProto.Item.Header.newBuilder()
                                           .setGraknVersion(Version.VERSION)
                                           .setOriginalDatabase(session.database().name()))
                        .build();
                write(outputStream, header);

                final List<Runnable> workers = new ArrayList<>();
                workers.add(() -> tx.concepts().getRootEntityType().getInstances().forEach(entity -> {
                    final DataProto.Item item = readEntity(entity);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootRelationType().getInstances().forEach(relation -> {
                    final DataProto.Item item = readRelation(relation);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootAttributeType().getInstances().forEach(attribute -> {
                    final DataProto.Item item = readAttribute(attribute);
                    write(outputStream, item);
                }));
                workers.parallelStream().forEach(Runnable::run);

                final DataProto.Item checksums = DataProto.Item.newBuilder().setChecksums(DataProto.Item.Checksums.newBuilder()
                                                                                                  .setEntityCount(entityCount.get())
                                                                                                  .setAttributeCount(attributeCount.get())
                                                                                                  .setRelationCount(relationCount.get())
                                                                                                  .setRoleCount(playerCount.get())
                                                                                                  .setOwnershipCount(ownershipCount.get()))
                        .build();
                write(outputStream, checksums);
            }
        } catch (final IOException e) {
            throw GraknException.of(FILE_NOT_WRITABLE, filename.toString());
        }
        LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                 entityCount.get(),
                 attributeCount.get(),
                 relationCount.get(),
                 playerCount.get(),
                 ownershipCount.get());
    }

    private DataProto.Item readEntity(final Entity entity) {
        entityCount.incrementAndGet();
        final DataProto.Item.Entity.Builder entityBuilder = DataProto.Item.Entity.newBuilder()
                .setId(new String(entity.getIID()))
                .setLabel(entity.getType().getLabel().name());
        readOwnerships(entity).forEach(a -> {
            ownershipCount.incrementAndGet();
            entityBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setEntity(entityBuilder).build();
    }

    private DataProto.Item readRelation(final Relation relation) {
        relationCount.incrementAndGet();
        final DataProto.Item.Relation.Builder relationBuilder = DataProto.Item.Relation.newBuilder()
                .setId(new String(relation.getIID()))
                .setLabel(relation.getType().getLabel().name());
        final Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        for (final Map.Entry<? extends RoleType, ? extends List<? extends Thing>> rolePlayers : playersByRole.entrySet()) {
            final RoleType role = rolePlayers.getKey();
            final DataProto.Item.Relation.Role.Builder roleBuilder = DataProto.Item.Relation.Role.newBuilder()
                    .setLabel(role.getLabel().scopedName());
            for (final Thing player : rolePlayers.getValue()) {
                playerCount.incrementAndGet();
                roleBuilder.addPlayer(DataProto.Item.Relation.Role.Player.newBuilder()
                                              .setId(new String(player.getIID())));
            }
            relationBuilder.addRole(roleBuilder);
        }
        readOwnerships(relation).forEach(a -> {
            ownershipCount.incrementAndGet();
            relationBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setRelation(relationBuilder).build();
    }

    private DataProto.Item readAttribute(final Attribute attribute) {
        attributeCount.incrementAndGet();
        final DataProto.Item.Attribute.Builder attributeBuilder = DataProto.Item.Attribute.newBuilder()
                .setId(new String(attribute.getIID()))
                .setLabel(attribute.getType().getLabel().name())
                .setValue(readValue(attribute));
        readOwnerships(attribute).forEach(a -> {
            ownershipCount.incrementAndGet();
            attributeBuilder.addAttribute(a);
        });
        return DataProto.Item.newBuilder().setAttribute(attributeBuilder).build();
    }

    private DataProto.ValueObject.Builder readValue(final Attribute attribute) {
        final DataProto.ValueObject.Builder valueObject = DataProto.ValueObject.newBuilder();
        if (attribute instanceof Attribute.String) {
            valueObject.setString(attribute.asString().getValue());
        } else if (attribute instanceof Attribute.Boolean) {
            valueObject.setBoolean(attribute.asBoolean().getValue());
        } else if (attribute instanceof Attribute.Long) {
            valueObject.setLong(attribute.asLong().getValue());
        } else if (attribute instanceof Attribute.Double) {
            valueObject.setDouble(attribute.asDouble().getValue());
        } else if (attribute instanceof Attribute.DateTime) {
            valueObject.setDatetime(attribute.asDateTime().getValue().atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
        return valueObject;
    }

    private Stream<DataProto.Item.OwnedAttribute.Builder> readOwnerships(final Thing thing) {
        return thing.getHas().map(attribute -> DataProto.Item.OwnedAttribute.newBuilder()
                .setId(new String(attribute.getIID())));
    }

    private synchronized void write(final OutputStream outputStream, final DataProto.Item item) {
        try {
            item.writeDelimitedTo(outputStream);
        } catch (final IOException e) {
            throw GraknException.of(FILE_NOT_WRITABLE, filename.toString());
        }
    }
}
