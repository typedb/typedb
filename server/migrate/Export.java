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
import java.util.stream.Stream;

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

    public Export(Session session, Path outputDir) throws IOException {
        this.session = session;

        Files.createDirectories(outputDir);

        Path outputFile = outputDir.resolve(session.keyspace().name() + ".grakn");
        this.outputStream = new BufferedOutputStream(Files.newOutputStream(outputFile));
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
        writeOwnerships();

        write(MigrateProto.Item.newBuilder()
                .setChecksums(MigrateProto.Item.Checksums.newBuilder()
                        .setEntityCount(entityCount)
                        .setAttributeCount(attributeCount)
                        .setRelationCount(relationCount)
                        .setRoleCount(roleCount)
                        .setOwnershipCount(ownershipCount)
                        .setKeyOwnershipCount(keyOwnershipCount))
                .build());

        outputStream.flush();

        LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships, {} key ownerships",
                entityCount,
                attributeCount,
                relationCount,
                roleCount,
                ownershipCount,
                keyOwnershipCount);
    }

    private void write(MigrateProto.Item item) throws IOException {
        item.writeDelimitedTo(outputStream);
    }

    private void writeAttributes() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ))
        {
            Stream<Attribute<?>> attributes = tx.getMetaAttributeType().instances();

            Iterator<Attribute<?>> iterator = attributes.iterator();
            while (iterator.hasNext()) {
                Attribute<?> attribute = iterator.next();
                AttributeType<?> attributeType = attribute.type();

                MigrateProto.Item.Attribute.Builder attributeBuilder = MigrateProto.Item.Attribute.newBuilder()
                        .setId(attribute.id().toString())
                        .setLabel(attributeType.label().toString())
                        .setValue(valueOf(attribute.value()));

                attribute.keys(attributeType.keys().toArray(AttributeType[]::new))
                        .forEach(key -> {
                            attributeBuilder.addKeys(
                                    MigrateProto.Item.Key.newBuilder()
                                            .setId(key.id().toString()));
                            keyOwnershipCount++;
                        });

                write(MigrateProto.Item.newBuilder().setAttribute(attributeBuilder).build());
                attributeCount++;
            }
        }
    }

    private void writeEntities() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Stream<Entity> entities = tx.getMetaEntityType().instances();

            Iterator<Entity> iterator = entities.iterator();
            while (iterator.hasNext()) {
                Entity entity = iterator.next();
                EntityType entityType = entity.type();

                MigrateProto.Item.Entity.Builder entityBuilder = MigrateProto.Item.Entity.newBuilder()
                        .setId(entity.id().toString())
                        .setLabel(entityType.label().toString());

                entity.keys(entityType.keys().toArray(AttributeType[]::new))
                        .forEach(key -> {
                            entityBuilder.addKeys(
                                    MigrateProto.Item.Key.newBuilder()
                                            .setId(key.id().toString()));
                            keyOwnershipCount++;
                        });

                write(MigrateProto.Item.newBuilder().setEntity(entityBuilder).build());
                entityCount++;
            }
        }
    }

    private void writeRelations() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Stream<Relation> relations = tx.getMetaRelationType().instances();

            Iterator<Relation> iterator = relations.iterator();
            while (iterator.hasNext()) {
                Relation relation = iterator.next();
                RelationType relationType = relation.type();
                String label = relationType.label().toString();
                if (label.startsWith("@")) {
                    continue;
                }

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

                relation.keys(relationType.keys().toArray(AttributeType[]::new))
                        .forEach(key -> {
                            relationBuilder.addKeys(
                                    MigrateProto.Item.Key.newBuilder()
                                            .setId(key.id().toString()));
                            keyOwnershipCount++;
                        });

                MigrateProto.Item item = MigrateProto.Item.newBuilder()
                        .setRelation(relationBuilder)
                        .build();

                write(item);
                relationCount++;
            }
        }
    }

    // TODO we shouldn't serialize keys here
    private long writeOwnerships() throws IOException {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {

            Stream<Attribute<?>> attributes = tx.getMetaAttributeType().instances();

            long count = 0;
            Iterator<Attribute<?>> attributeIterator = attributes.iterator();
            while (attributeIterator.hasNext()) {
                Attribute<?> attribute = attributeIterator.next();

                String attributeId = attribute.id().toString();

                Stream<Thing> owners = attribute.owners();
                Iterator<Thing> ownerIterator = owners.iterator();
                while (ownerIterator.hasNext()) {
                    Thing owner = ownerIterator.next();

                    // Ignore key ownership, this is the thing's responsibility
                    if (owner.keys(attribute.type()).findAny().isPresent()) {
                        continue;
                    }

                    MigrateProto.Item item = MigrateProto.Item.newBuilder()
                            .setOwnership(MigrateProto.Item.Ownership.newBuilder()
                                    .setAttributeId(attributeId)
                                    .setOwnerId(owner.id().toString())
                            ).build();

                    write(item);
                    ownershipCount++;
                }
            }

            return count;
        }
    }

    private MigrateProto.ValueObject.Builder valueOf(Object value) {
        MigrateProto.ValueObject.Builder valueObject = MigrateProto.ValueObject.newBuilder();
        if (value instanceof String) {
            valueObject.setString((String) value);
        } else if (value instanceof Boolean) {
            valueObject.setBoolean((Boolean) value);
        } else if (value instanceof Integer) {
            valueObject.setInteger((Integer) value);
        } else if (value instanceof Long) {
            valueObject.setLong((Long) value);
        } else if (value instanceof Float) {
            valueObject.setFloat((Float) value);
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
