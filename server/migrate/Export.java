/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.migrate;

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.server.Version;
import grakn.core.server.migrate.proto.MigrateProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Export implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    private final Session session;
    private final BufferedOutputStream outputStream;

    AtomicLong attributeCount = new AtomicLong(0);
    AtomicLong entityCount = new AtomicLong(0);
    AtomicLong relationCount = new AtomicLong(0);
    AtomicLong ownershipCount = new AtomicLong(0);
    AtomicLong roleCount = new AtomicLong(0);

    public Export(Session session, Path output) throws IOException {
        this.session = session;

        Files.createDirectories(output.getParent());

        this.outputStream = new BufferedOutputStream(Files.newOutputStream(output));
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    public void export() throws IOException {

        write(MigrateProto.Item.newBuilder()
                .setHeader(MigrateProto.Item.Header.newBuilder()
                        .setGraknVersion(Version.VERSION)
                        .setOriginalKeyspace(session.keyspace().name()))
                .build());

        writeAttributes();
        writeEntities();
        writeRelations();

        write(MigrateProto.Item.newBuilder()
                .setChecksums(MigrateProto.Item.Checksums.newBuilder()
                        .setEntityCount(entityCount.get())
                        .setAttributeCount(attributeCount.get())
                        .setRelationCount(relationCount.get())
                        .setRoleCount(roleCount.get())
                        .setOwnershipCount(ownershipCount.get()))
                .build());

        synchronized (outputStream) {
            outputStream.flush();
        }

        LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                entityCount,
                attributeCount,
                relationCount,
                roleCount,
                ownershipCount);
    }

    private void write(MigrateProto.Item item) throws IOException {
        synchronized (outputStream) {
            item.writeDelimitedTo(outputStream);
        }
    }

    private void writeAttributes() {
        List<String> attributeLabels;

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            attributeLabels = ((Stream<AttributeType<?>>) tx.getMetaAttributeType().subs())
                    .filter(at -> !at.isAbstract() && !at.isImplicit())
                    .map(at -> at.label().toString())
                    .collect(Collectors.toList());
        }

        attributeLabels.parallelStream().forEach(label -> {
            try {
                writeAttributeInstancesOfType(label);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private <D> void writeAttributeInstancesOfType(String label) throws IOException {
        LOG.info("Writing attribute type: {}", label);

        long localOwnershipCount = 0;
        long localAttributeCount = 0;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            AttributeType<D> attributeType = tx.getAttributeType(label);
            AttributeType<?>[] ownedTypes = attributeType.attributes().toArray(AttributeType[]::new);
            Iterator<Attribute<D>> attributes = attributeType.instancesDirect().iterator();
            while (attributes.hasNext()) {
                Attribute<D> attribute = attributes.next();

                MigrateProto.Item.Attribute.Builder attributeBuilder = MigrateProto.Item.Attribute.newBuilder()
                        .setId(attribute.id().toString())
                        .setLabel(label)
                        .setValue(valueOf(attribute.value()));

                localOwnershipCount += attribute.attributes(ownedTypes).peek(a -> {
                    attributeBuilder.addAttribute(
                            MigrateProto.Item.OwnedAttribute.newBuilder()
                                    .setId(a.id().toString()));
                }).count();

                write(MigrateProto.Item.newBuilder().setAttribute(attributeBuilder).build());
                localAttributeCount++;
                if (localAttributeCount % 1_000 == 0) {
                    LOG.info("Exported {}: count: {}, ownerships: {}", label, localAttributeCount, localOwnershipCount);
                }
            }
        }

        LOG.info("Exported {}: count: {}, ownerships: {}", label, localAttributeCount, localOwnershipCount);
        ownershipCount.addAndGet(localOwnershipCount);
        attributeCount.addAndGet(localAttributeCount);
    }

    private void writeEntities() {
        List<String> entityLabels;

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            entityLabels = tx.getMetaEntityType().subs()
                    .filter(et -> !et.isAbstract() && !et.isImplicit())
                    .map(et -> et.label().toString())
                    .collect(Collectors.toList());
        }

        entityLabels.parallelStream().forEach(label -> {
            try {
                writeEntityInstancesOfType(label);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void writeEntityInstancesOfType(String label) throws IOException {
        LOG.info("Writing entity type: {}", label);

        long localOwnershipCount = 0;
        long localEntityCount = 0;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            EntityType entityType = tx.getEntityType(label);
            AttributeType<?>[] ownedTypes = entityType.attributes().toArray(AttributeType[]::new);
            Iterator<Entity> entities = entityType.instancesDirect().iterator();
            while (entities.hasNext()) {
                Entity entity = entities.next();

                MigrateProto.Item.Entity.Builder entityBuilder = MigrateProto.Item.Entity.newBuilder()
                        .setId(entity.id().toString())
                        .setLabel(label);

                localOwnershipCount += entity.attributes(ownedTypes).peek(a -> {
                    entityBuilder.addAttribute(
                            MigrateProto.Item.OwnedAttribute.newBuilder()
                                    .setId(a.id().toString()));
                }).count();

                write(MigrateProto.Item.newBuilder().setEntity(entityBuilder).build());
                localEntityCount++;
                if (localEntityCount % 1_000 == 0) {
                    LOG.info("Exported {}: count: {}, ownerships: {}", label, localEntityCount, localOwnershipCount);
                }
            }
        }

        LOG.info("Exported {}: count: {}, ownerships: {}", label, localEntityCount, localOwnershipCount);

        ownershipCount.addAndGet(localOwnershipCount);
        entityCount.addAndGet(localEntityCount);
    }

    private void writeRelations() {
        List<String> relationLabels;

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            relationLabels = tx.getMetaRelationType().subs()
                    .filter(rt -> !rt.isAbstract() && !rt.isImplicit())
                    .map(rt -> rt.label().toString())
                    .collect(Collectors.toList());
        }

        relationLabels.parallelStream().forEach(label -> {
            try {
                writeRelationInstancesOfType(label);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void writeRelationInstancesOfType(String label) throws IOException {
        LOG.info("Writing relation type: {}", label);

        long localOwnershipCount = 0;
        long localRelationCount = 0;
        long localRoleCount = 0;
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            RelationType relationType = tx.getRelationType(label);
            AttributeType<?>[] ownedTypes = relationType.attributes().toArray(AttributeType[]::new);
            Iterator<Relation> relations = relationType.instancesDirect().iterator();
            while (relations.hasNext()) {
                Relation relation = relations.next();

                MigrateProto.Item.Relation.Builder relationBuilder = MigrateProto.Item.Relation.newBuilder()
                        .setId(relation.id().toString())
                        .setLabel(label);

                Map<Role, List<Thing>> roleMap = relation.rolePlayersMap();
                for (Map.Entry<Role, List<Thing>> roleEntry : roleMap.entrySet()) {
                    Role role = roleEntry.getKey();
                    if (role.isImplicit()) {
                        continue;
                    }

                    MigrateProto.Item.Relation.Role.Builder roleBuilder = MigrateProto.Item.Relation.Role.newBuilder()
                            .setLabel(role.label().toString());

                    for (Thing player : roleEntry.getValue()) {
                        roleBuilder.addPlayer(MigrateProto.Item.Relation.Role.Player.newBuilder()
                                .setId(player.id().toString()));
                        localRoleCount++;
                    }

                    relationBuilder.addRole(roleBuilder);
                }

                localOwnershipCount += relation.attributes(ownedTypes)
                        .peek(a -> relationBuilder.addAttribute(
                                MigrateProto.Item.OwnedAttribute.newBuilder()
                                        .setId(a.id().toString())))
                        .count();

                MigrateProto.Item item = MigrateProto.Item.newBuilder()
                        .setRelation(relationBuilder)
                        .build();

                write(item);
                localRelationCount++;
                if (localRelationCount % 1_000 == 0) {
                    LOG.info("Exported {}: count: {}, ownerships: {}, roles: {}", label, localRelationCount, localOwnershipCount, localRoleCount);
                }
            }
        }

        LOG.info("Exported {}: count: {}, ownerships: {}, roles: {}", label, localRelationCount, localOwnershipCount, localRoleCount);
        roleCount.addAndGet(localRoleCount);
        ownershipCount.addAndGet(localOwnershipCount);
        relationCount.addAndGet(localRelationCount);
    }

    private MigrateProto.ValueObject.Builder valueOf(Object value) {
        MigrateProto.ValueObject.Builder valueObject = MigrateProto.ValueObject.newBuilder();
        if (value instanceof String) {
            valueObject.setString((String) value);
        } else if (value instanceof Boolean) {
            valueObject.setBoolean((Boolean) value);
        } else if (value instanceof Integer) {
            valueObject.setLong((Integer) value);
        } else if (value instanceof Long) {
            valueObject.setLong((Long) value);
        } else if (value instanceof Float) {
            valueObject.setDouble((Float) value);
        } else if (value instanceof Double) {
            valueObject.setDouble((Double) value);
        } else if (value instanceof LocalDateTime) {
            valueObject.setDatetime(((LocalDateTime) value).atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw new IllegalArgumentException("Value was not of a valid type");
        }
        return valueObject;
    }
}
