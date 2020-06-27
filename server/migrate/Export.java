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

public class Export implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    private final Session session;
    private final BufferedOutputStream outputStream;

    long attributeCount = 0;
    long entityCount = 0;
    long relationCount = 0;
    long ownershipCount = 0;
    long keyOwnershipCount = 0;
    long roleCount = 0;

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
                        .setEntityCount(entityCount)
                        .setAttributeCount(attributeCount)
                        .setRelationCount(relationCount)
                        .setRoleCount(roleCount)
                        .setOwnershipCount(ownershipCount))
                .build());

        outputStream.flush();

        LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                entityCount,
                attributeCount,
                relationCount,
                roleCount,
                ownershipCount);
    }

    private void write(MigrateProto.Item item) throws IOException {
        item.writeDelimitedTo(outputStream);
    }

    private void writeAttributes() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Iterator<AttributeType<?>> attributeTypes = tx.getMetaAttributeType().subs().iterator();
            while (attributeTypes.hasNext()) {
                AttributeType<?> attributeType = attributeTypes.next();

                if (attributeType.isAbstract()) {
                    continue;
                }

                writeAttributeInstancesOfType(attributeType);
            }
        }
    }

    private <D> void writeAttributeInstancesOfType(AttributeType<D> attributeType) throws IOException {
        String label = attributeType.label().toString();
        LOG.info("Writing attribute type: {}", label);

        AttributeType<?>[] ownedTypes = attributeType.attributes().toArray(AttributeType[]::new);

        Iterator<Attribute<D>> attributes = attributeType.instancesDirect().iterator();
        while (attributes.hasNext()) {
            Attribute<D> attribute = attributes.next();

            MigrateProto.Item.Attribute.Builder attributeBuilder = MigrateProto.Item.Attribute.newBuilder()
                    .setId(attribute.id().toString())
                    .setLabel(label)
                    .setValue(valueOf(attribute.value()));

            attribute.attributes(ownedTypes).forEach(a -> {
                attributeBuilder.addAttribute(
                        MigrateProto.Item.OwnedAttribute.newBuilder()
                                .setId(a.id().toString()));
                ownershipCount++;
            });

            write(MigrateProto.Item.newBuilder().setAttribute(attributeBuilder).build());
            attributeCount++;
        }
    }

    private void writeEntities() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Iterator<? extends EntityType> entityTypes = tx.getMetaEntityType().subs().iterator();
            while (entityTypes.hasNext()) {
                EntityType entityType = entityTypes.next();

                if (entityType.isAbstract()) {
                    continue;
                }

                writeEntityInstancesOfType(entityType);
            }
        }
    }

    private void writeEntityInstancesOfType(EntityType entityType) throws IOException {
        String label = entityType.label().toString();
        LOG.info("Writing entity type: {}", label);

        AttributeType<?>[] ownedTypes = entityType.attributes().toArray(AttributeType[]::new);

        Iterator<Entity> entities = entityType.instancesDirect().iterator();
        while (entities.hasNext()) {
            Entity entity = entities.next();

            MigrateProto.Item.Entity.Builder entityBuilder = MigrateProto.Item.Entity.newBuilder()
                    .setId(entity.id().toString())
                    .setLabel(label);

            entity.attributes(ownedTypes).forEach(a -> {
                entityBuilder.addAttribute(
                        MigrateProto.Item.OwnedAttribute.newBuilder()
                                .setId(a.id().toString()));
                ownershipCount++;
            });

            write(MigrateProto.Item.newBuilder().setEntity(entityBuilder).build());
            entityCount++;
        }
    }

    private void writeRelations() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Iterator<? extends RelationType> relationTypes = tx.getMetaRelationType().subs().iterator();
            while (relationTypes.hasNext()) {
                RelationType relationType = relationTypes.next();

                if (relationType.isAbstract()) {
                    continue;
                }

                // Ignore meta relations (inferred has)
                if (relationType.label().toString().startsWith("@")) {
                    continue;
                }

                writeRelationInstancesOfType(relationType);
            }
        }
    }

    private void writeRelationInstancesOfType(RelationType relationType) throws IOException {
        String label = relationType.label().toString();
        LOG.info("Writing relation type: {}", label);

        AttributeType<?>[] ownedTypes = relationType.attributes().toArray(AttributeType[]::new);

        Iterator<Relation> relations = relationType.instancesDirect().iterator();
        while (relations.hasNext()) {
            Relation relation = relations.next();

            MigrateProto.Item.Relation.Builder relationBuilder = MigrateProto.Item.Relation.newBuilder()
                    .setId(relation.id().toString())
                    .setLabel(label);

            Map<Role, List<Thing>> roleMap = relation.rolePlayersMap();
            for (Map.Entry<Role, List<Thing>> roleEntry : roleMap.entrySet()) {

                MigrateProto.Item.Relation.Role.Builder roleBuilder = MigrateProto.Item.Relation.Role.newBuilder()
                        .setLabel(roleEntry.getKey().label().toString());

                for (Thing player : roleEntry.getValue()) {
                    roleBuilder.addPlayer(MigrateProto.Item.Relation.Role.Player.newBuilder()
                            .setId(player.id().toString()));
                }

                relationBuilder.addRole(roleBuilder);
                roleCount++;
            }

            relation.attributes(ownedTypes).forEach(a -> {
                relationBuilder.addAttribute(
                        MigrateProto.Item.OwnedAttribute.newBuilder()
                                .setId(a.id().toString()));
                ownershipCount++;
            });

            MigrateProto.Item item = MigrateProto.Item.newBuilder()
                    .setRelation(relationBuilder)
                    .build();

            write(item);
            relationCount++;
        }
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
