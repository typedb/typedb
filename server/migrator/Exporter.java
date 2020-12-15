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
    private final Grakn.Session session;
    private final Path filename;
    private final AtomicLong entityCount = new AtomicLong(0);
    private final AtomicLong relationCount = new AtomicLong(0);
    private final AtomicLong attributeCount = new AtomicLong(0);
    private final AtomicLong ownershipCount = new AtomicLong(0);
    private final AtomicLong playerCount = new AtomicLong(0);

    public Exporter(Grakn grakn, String database, Path filename) {
        this.session = grakn.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        long current = attributeCount.get() + relationCount.get() + entityCount.get();
        return MigratorProto.Job.Progress.newBuilder()
                .setCurrent(current)
                // TODO
                .setTotal(100000000)
                .build();
    }

    @Override
    public void run() {
        LOG.info("Exporting {} from Grakn {}", session.database().name(), Version.VERSION);
        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(filename))) {
            try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                DataProto.Item header = DataProto.Item.newBuilder()
                        .setHeader(DataProto.Item.Header.newBuilder()
                                .setGraknVersion(Version.VERSION)
                                .setOriginalDatabase(session.database().name()))
                        .build();
                write(outputStream, header);

                List<Runnable> workers = new ArrayList<>();
                workers.add(() -> tx.concepts().getRootEntityType().getInstances().forEach(entity -> {
                    DataProto.Item item = readEntity(entity);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootRelationType().getInstances().forEach(relation -> {
                    DataProto.Item item = readRelation(relation);
                    write(outputStream, item);
                }));
                workers.add(() -> tx.concepts().getRootAttributeType().getInstances().forEach(attribute -> {
                    DataProto.Item item = readAttribute(attribute);
                    write(outputStream, item);
                }));
                workers.parallelStream().forEach(Runnable::run);

                DataProto.Item checksums = DataProto.Item.newBuilder().setChecksums(DataProto.Item.Checksums.newBuilder()
                        .setEntityCount(entityCount.get())
                        .setAttributeCount(attributeCount.get())
                        .setRelationCount(relationCount.get())
                        .setRoleCount(playerCount.get())
                        .setOwnershipCount(ownershipCount.get()))
                        .build();
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
        readOwnerships(entity).forEach(entityBuilder::addAttribute);
        return DataProto.Item.newBuilder().setEntity(entityBuilder).build();
    }

    private DataProto.Item readRelation(Relation relation) {
        relationCount.incrementAndGet();
        DataProto.Item.Relation.Builder relationBuilder = DataProto.Item.Relation.newBuilder()
                .setId(new String(relation.getIID()))
                .setLabel(relation.getType().getLabel().name());
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        for (Map.Entry<? extends RoleType, ? extends  List<? extends Thing>> rolePlayers : playersByRole.entrySet()) {
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
        readOwnerships(relation).forEach(relationBuilder::addAttribute);
        return DataProto.Item.newBuilder().setRelation(relationBuilder).build();
    }

    private DataProto.Item readAttribute(Attribute attribute) {
        attributeCount.incrementAndGet();
        DataProto.Item.Attribute.Builder attributeBuilder = DataProto.Item.Attribute.newBuilder()
                .setId(new String(attribute.getIID()))
                .setLabel(attribute.getType().getLabel().name())
                .setValue(readValue(attribute));
        readOwnerships(attribute).forEach(attributeBuilder::addAttribute);
        return DataProto.Item.newBuilder().setAttribute(attributeBuilder).build();
    }

    private DataProto.ValueObject.Builder readValue(Attribute attribute) {
        DataProto.ValueObject.Builder valueObject = DataProto.ValueObject.newBuilder();
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

    private Stream<DataProto.Item.OwnedAttribute.Builder> readOwnerships(Thing thing) {
        return thing.getHas().map(attribute -> {
            ownershipCount.incrementAndGet();
            return DataProto.Item.OwnedAttribute.newBuilder()
                    .setId(new String(thing.getIID()));
        });
    }

    private synchronized void write(OutputStream outputStream, DataProto.Item item) {
        try {
            item.writeDelimitedTo(outputStream);
        } catch (IOException e) {
            throw GraknException.of(FILE_NOT_WRITABLE, filename.toString());
        }
    }
}
