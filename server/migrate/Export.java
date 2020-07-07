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
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.server.Version;
import grakn.core.server.migrate.proto.MigrateProto;
import graql.lang.Graql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Export implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final Session session;
    private final BufferedOutputStream outputStream;

    private final Counter counter = new Counter();

    private final long approximateThingCount;

    public Export(Session session, Path output) throws IOException {
        this.session = session;

        Files.createDirectories(output.getParent());

        this.outputStream = new BufferedOutputStream(Files.newOutputStream(output));

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            long thingCount = tx.execute(Graql.compute().count().in("thing"))
                    .stream().findFirst().map(n -> n.number().longValue()).orElse(0L);
            long implicitRelationCount = tx.execute(Graql.compute().count().in("@has-attribute"))
                    .stream().findFirst().map(n -> n.number().longValue()).orElse(0L);
            long tempApproximateThingCount = thingCount - implicitRelationCount;

            // Ensure we don't report a negative count due to bad compute, as that is confusing UX
            approximateThingCount = tempApproximateThingCount > 0 ? tempApproximateThingCount : 0;
        }

        LOG.info("Approximate thing count {}", approximateThingCount);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    public class Progress {
        private final long estimatedTotal;
        private final long currentProgress;

        private Progress(long currentProgress) {
            this.currentProgress = currentProgress;
            estimatedTotal = Math.max(currentProgress, approximateThingCount);
        }

        public long getCurrentProgress() {
            return currentProgress;
        }

        public long getEstimatedTotal() {
            return estimatedTotal;
        }

        public double percentCompletion() {
            return (double) currentProgress / (double) estimatedTotal;
        }
    }

    /**
     * Thread-safe way of retrieving current progress.
     *
     * @return Current progress
     */
    public Progress getCurrentProgress() {
        return new Progress(counter.getThingCount());
    }

    /**
     * Called by the main worker thread and blocks until export completion.
     * @throws IOException
     */
    public void export() throws IOException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Cannot invoke same export twice");
        }

        LOG.info("Exporting {} from Grakn {}", session.keyspace().name(), Version.VERSION);

        write(MigrateProto.Item.newBuilder()
                .setHeader(MigrateProto.Item.Header.newBuilder()
                        .setGraknVersion(Version.VERSION)
                        .setOriginalKeyspace(session.keyspace().name()))
                .build());

        List<ExportWorker<?, ?>> exportWorkers = new ArrayList<>();
        exportWorkers.addAll(createAttributeWorkers());
        exportWorkers.addAll(createEntityWorkers());
        exportWorkers.addAll(createRelationWorkers());

        exportWorkers.parallelStream().forEach(ExportWorker::export);

        write(MigrateProto.Item.newBuilder()
                .setChecksums(counter.getChecksums())
                .build());

        outputStream.flush();

        counter.logExported();
    }

    private void write(MigrateProto.Item item) throws IOException {
        synchronized (outputStream) {
            item.writeDelimitedTo(outputStream);
        }
    }

    private List<AttributeExportWorker<?>> createAttributeWorkers() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            return ((Stream<AttributeType<?>>) tx.getMetaAttributeType().subs())
                    .filter(at -> !at.isAbstract() && !at.isImplicit())
                    .map(at -> at.label().toString())
                    .map(AttributeExportWorker::new)
                    .collect(Collectors.toList());
        }
    }

    private List<EntityExportWorker> createEntityWorkers() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            return tx.getMetaEntityType().subs()
                    .filter(et -> !et.isAbstract() && !et.isImplicit())
                    .map(et -> et.label().toString())
                    .map(EntityExportWorker::new)
                    .collect(Collectors.toList());
        }
    }

    private List<RelationExportWorker> createRelationWorkers() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            return tx.getMetaRelationType().subs()
                    .filter(rt -> !rt.isAbstract() && !rt.isImplicit())
                    .map(rt -> rt.label().toString())
                    .map(RelationExportWorker::new)
                    .collect(Collectors.toList());
        }
    }

    private class Counter {
        private AtomicLong attributeCount = new AtomicLong(0);
        private AtomicLong entityCount = new AtomicLong(0);
        private AtomicLong relationCount = new AtomicLong(0);
        private AtomicLong ownershipCount = new AtomicLong(0);
        private AtomicLong roleCount = new AtomicLong(0);

        void addAttribute() {
            attributeCount.incrementAndGet();
        }

        void addEntity() {
            entityCount.incrementAndGet();
        }

        void addRelation() {
            relationCount.incrementAndGet();
        }

        void addOwnership() {
            ownershipCount.incrementAndGet();
        }

        void addRole() {
            roleCount.incrementAndGet();
        }

        long getThingCount() {
            return attributeCount.get() + entityCount.get() + relationCount.get();
        }

        MigrateProto.Item.Checksums.Builder getChecksums() {
            return MigrateProto.Item.Checksums.newBuilder()
                    .setEntityCount(entityCount.get())
                    .setAttributeCount(attributeCount.get())
                    .setRelationCount(relationCount.get())
                    .setRoleCount(roleCount.get())
                    .setOwnershipCount(ownershipCount.get());
        }

        public void logExported() {
            LOG.info("Exported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                    entityCount,
                    attributeCount,
                    relationCount,
                    roleCount,
                    ownershipCount);
        }
    }

    private abstract class ExportWorker<T extends Type, U extends Thing> {
        private final String label;

        ExportWorker(String label) {
            this.label = label;
        }

        final void export() {
            LOG.debug(">>>>> Exporting: {}", label);

            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                T type = tx.getConcept(ConceptId.of(label));
                AttributeType<?>[] ownedTypes = type.attributes().filter(at -> !at.isImplicit()).toArray(AttributeType[]::new);
                Iterator<U> iterator = (Iterator<U>) type.instancesDirect().iterator();
                while (iterator.hasNext()) {
                    write(read(label, iterator.next(), ownedTypes));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            LOG.debug("||||| Finished: {}", label);
        }

        abstract protected MigrateProto.Item read(String label, U instance, AttributeType<?>[] ownedTypes);
    }

    private class AttributeExportWorker<D> extends ExportWorker<AttributeType<D>, Attribute<D>> {
        AttributeExportWorker(String label) {
            super(label);
        }

        @Override
        protected MigrateProto.Item read(String label, Attribute<D> attribute, AttributeType<?>[] ownedTypes) {
            MigrateProto.Item.Attribute.Builder attributeBuilder = MigrateProto.Item.Attribute.newBuilder()
                    .setId(attribute.id().toString())
                    .setLabel(label)
                    .setValue(valueOf(attribute.value()));

            attribute.attributes(ownedTypes).forEach(a -> {
                counter.addOwnership();
                attributeBuilder.addAttribute(
                        MigrateProto.Item.OwnedAttribute.newBuilder()
                                .setId(a.id().toString()));
            });

            counter.addAttribute();
            return MigrateProto.Item.newBuilder().setAttribute(attributeBuilder).build();
        }
    }

    private class EntityExportWorker extends ExportWorker<EntityType, Entity> {
        EntityExportWorker(String label) {
            super(label);
        }

        @Override
        protected MigrateProto.Item read(String label, Entity entity, AttributeType<?>[] ownedTypes) {
            MigrateProto.Item.Entity.Builder entityBuilder = MigrateProto.Item.Entity.newBuilder()
                    .setId(entity.id().toString())
                    .setLabel(label);

            readOwnerships(entity, ownedTypes).forEach(entityBuilder::addAttribute);

            counter.addEntity();
            return MigrateProto.Item.newBuilder().setEntity(entityBuilder).build();
        }
    }

    private class RelationExportWorker extends ExportWorker<RelationType, Relation> {
        RelationExportWorker(String label) {
            super(label);
        }

        @Override
        protected MigrateProto.Item read(String label, Relation relation, AttributeType<?>[] ownedTypes) {
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
                    counter.addRole();
                }

                relationBuilder.addRole(roleBuilder);
            }

            readOwnerships(relation, ownedTypes).forEach(relationBuilder::addAttribute);

            counter.addRelation();

            return MigrateProto.Item.newBuilder()
                    .setRelation(relationBuilder)
                    .build();
        }
    }

    private Stream<MigrateProto.Item.OwnedAttribute.Builder> readOwnerships(Thing thing, AttributeType<?>[] ownedTypes) {
        return thing.attributes(ownedTypes).map(a -> {
            counter.addOwnership();
            return MigrateProto.Item.OwnedAttribute.newBuilder()
                    .setId(a.id().toString());
        });
    }

    private static MigrateProto.ValueObject.Builder valueOf(Object value) {
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
