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
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.server.Version;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.migrate.proto.DataProto;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.session.SessionFactory;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Export extends AbstractJob {
    private static final Logger LOG = LoggerFactory.getLogger(Export.class);

    private final SessionFactory sessionFactory;
    private final Path outputPath;
    private final String keyspace;

    private Session session;
    private BufferedOutputStream outputStream;

    private final Counter counter = new Counter();

    private long approximateThingCount;

    public Export(SessionFactory sessionFactory, Path outputPath, String keyspace) {
        super("export");
        this.sessionFactory = sessionFactory;
        this.outputPath = outputPath;
        this.keyspace = keyspace;
    }

    /**
     * Thread-safe way of retrieving current progress.
     *
     * @return Current progress
     */
    @Override
    public MigrateProto.Job.Progress getCurrentProgress() {
        long current = counter.getThingCount();
        return MigrateProto.Job.Progress.newBuilder()
                .setTotalCount(Math.max(current, approximateThingCount))
                .setCurrentProgress(current)
                .build();
    }

    @Override
    public MigrateProto.Job.Completion getCompletion() {
        return MigrateProto.Job.Completion.newBuilder().setTotalCount(approximateThingCount).build();
    }

    /**
     * Called by the main worker thread and blocks until export completion.
     */
    @Override
    protected void executeInternal() throws Exception {
        LOG.info("Exporting {} from Grakn {}", keyspace, Version.VERSION);

        try (Session session = sessionFactory.session(new KeyspaceImpl(keyspace))) {
            this.session = session;

            computeApproximateCount();

            Files.createDirectories(outputPath.getParent());

            this.outputStream = new BufferedOutputStream(Files.newOutputStream(outputPath));

            write(DataProto.Item.newBuilder()
                    .setHeader(DataProto.Item.Header.newBuilder()
                            .setGraknVersion(Version.VERSION)
                            .setOriginalKeyspace(session.keyspace().name()))
                    .build());

            List<ExportWorker<?, ?>> exportWorkers = new ArrayList<>();
            exportWorkers.addAll(createAttributeWorkers());
            exportWorkers.addAll(createEntityWorkers());
            exportWorkers.addAll(createRelationWorkers());

            exportWorkers.parallelStream().forEach(ExportWorker::export);

            write(DataProto.Item.newBuilder()
                    .setChecksums(counter.getChecksums())
                    .build());

            outputStream.flush();

            counter.logExported();
        }
    }

    private void computeApproximateCount() {
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            // Compute is broken, we must manually prune implicit relations from the types to count as their counts are
            // completely wrong. We must also ensure that we only add supertypes, so we check that included relation
            // types are definitely direct subtypes of "relation".
            RelationType metaRelationType = tx.getMetaRelationType();
            List<String> nonImplicitTypes = Stream.concat(metaRelationType.subs()
                            .filter(rt -> !rt.equals(metaRelationType) && Objects.equals(rt.sup(), metaRelationType) && !rt.isImplicit())
                            .map(SchemaConcept::label)
                            .map(Label::toString),
                    Stream.of("attribute", "entity"))
                    .collect(Collectors.toList());

            approximateThingCount = tx.execute(Graql.compute().count().in(nonImplicitTypes))
                    .stream().findFirst().map(n -> n.number().longValue()).orElse(0L);
        }
    }

    private synchronized void write(DataProto.Item item) throws IOException {
        item.writeDelimitedTo(outputStream);
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

    private static class Counter {
        private final AtomicLong attributeCount = new AtomicLong(0);
        private final AtomicLong entityCount = new AtomicLong(0);
        private final AtomicLong relationCount = new AtomicLong(0);
        private final AtomicLong ownershipCount = new AtomicLong(0);
        private final AtomicLong roleCount = new AtomicLong(0);

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

        DataProto.Item.Checksums.Builder getChecksums() {
            return DataProto.Item.Checksums.newBuilder()
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
                T type = tx.getSchemaConcept(Label.of(label));
                AttributeType<?>[] ownedTypes = type.attributes().filter(at -> !at.isImplicit()).toArray(AttributeType[]::new);
                Iterator<U> iterator = (Iterator<U>) type.instancesDirect().iterator();
                while (iterator.hasNext()) {
                    if (isCancelled()) {
                        throw new CancellationException();
                    }

                    write(read(label, iterator.next(), ownedTypes));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            LOG.debug("||||| Finished: {}", label);
        }

        abstract protected DataProto.Item read(String label, U instance, AttributeType<?>[] ownedTypes);
    }

    private class AttributeExportWorker<D> extends ExportWorker<AttributeType<D>, Attribute<D>> {
        AttributeExportWorker(String label) {
            super(label);
        }

        @Override
        protected DataProto.Item read(String label, Attribute<D> attribute, AttributeType<?>[] ownedTypes) {
            DataProto.Item.Attribute.Builder attributeBuilder = DataProto.Item.Attribute.newBuilder()
                    .setId(attribute.id().toString())
                    .setLabel(label)
                    .setValue(valueOf(attribute.value()));

            attribute.attributes(ownedTypes).forEach(a -> {
                counter.addOwnership();
                attributeBuilder.addAttribute(
                        DataProto.Item.OwnedAttribute.newBuilder()
                                .setId(a.id().toString()));
            });

            counter.addAttribute();
            return DataProto.Item.newBuilder().setAttribute(attributeBuilder).build();
        }
    }

    private class EntityExportWorker extends ExportWorker<EntityType, Entity> {
        EntityExportWorker(String label) {
            super(label);
        }

        @Override
        protected DataProto.Item read(String label, Entity entity, AttributeType<?>[] ownedTypes) {
            DataProto.Item.Entity.Builder entityBuilder = DataProto.Item.Entity.newBuilder()
                    .setId(entity.id().toString())
                    .setLabel(label);

            readOwnerships(entity, ownedTypes).forEach(entityBuilder::addAttribute);

            counter.addEntity();
            return DataProto.Item.newBuilder().setEntity(entityBuilder).build();
        }
    }

    private class RelationExportWorker extends ExportWorker<RelationType, Relation> {
        RelationExportWorker(String label) {
            super(label);
        }

        @Override
        protected DataProto.Item read(String label, Relation relation, AttributeType<?>[] ownedTypes) {
            DataProto.Item.Relation.Builder relationBuilder = DataProto.Item.Relation.newBuilder()
                    .setId(relation.id().toString())
                    .setLabel(label);

            Map<Role, List<Thing>> roleMap = relation.rolePlayersMap();
            for (Map.Entry<Role, List<Thing>> roleEntry : roleMap.entrySet()) {
                Role role = roleEntry.getKey();
                if (role.isImplicit()) {
                    continue;
                }

                DataProto.Item.Relation.Role.Builder roleBuilder = DataProto.Item.Relation.Role.newBuilder()
                        .setLabel(role.label().toString());

                for (Thing player : roleEntry.getValue()) {
                    roleBuilder.addPlayer(DataProto.Item.Relation.Role.Player.newBuilder()
                            .setId(player.id().toString()));
                    counter.addRole();
                }

                relationBuilder.addRole(roleBuilder);
            }

            readOwnerships(relation, ownedTypes).forEach(relationBuilder::addAttribute);

            counter.addRelation();

            return DataProto.Item.newBuilder()
                    .setRelation(relationBuilder)
                    .build();
        }
    }

    private Stream<DataProto.Item.OwnedAttribute.Builder> readOwnerships(Thing thing, AttributeType<?>[] ownedTypes) {
        return thing.attributes(ownedTypes).map(a -> {
            counter.addOwnership();
            return DataProto.Item.OwnedAttribute.newBuilder()
                    .setId(a.id().toString());
        });
    }

    private static DataProto.ValueObject.Builder valueOf(Object value) {
        DataProto.ValueObject.Builder valueObject = DataProto.ValueObject.newBuilder();
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
