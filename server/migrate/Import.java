package grakn.core.server.migrate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import grakn.common.util.Pair;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.server.Version;
import grakn.core.server.migrate.proto.MigrateProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Import implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    private static final int BATCH_SIZE = 10_000;

    private final Session session;
    private final InputStream inputStream;
    private final Parser<MigrateProto.Item> itemParser;

    private Transaction currentTransaction;
    private int count = 0;

    Map<String, String> idMap = new HashMap<>();

    // Pair of LOCAL attribute ids to FOREIGN key ids
    List<Pair<String, List<String>>> attributeOwnerships = new ArrayList<>();

    Map<String, AttributeType<?>> attributeTypeCache = new HashMap<>();
    Map<String, EntityType> entityTypeCache = new HashMap<>();
    Map<String, RelationType> relationTypeCache = new HashMap<>();
    Map<String, Attribute<?>> attributeCache = new HashMap<>();

    private long entityCount = 0;
    private long attributeCount = 0;
    private long relationCount = 0;
    private long ownershipCount = 0;
    private long roleCount = 0;

    public Import(Session session, Path inputFile) throws IOException {
        this.session = session;

        this.inputStream = new BufferedInputStream(Files.newInputStream(inputFile));
        this.itemParser = MigrateProto.Item.parser();
        // TODO read header here
    }

    @Override
    public void close() throws IOException {
        currentTransaction.close();
        inputStream.close();
    }

    private MigrateProto.Item read() throws InvalidProtocolBufferException {
        return itemParser.parseDelimitedFrom(inputStream);
    }

    private void write() {
        count++;
        if (count >= BATCH_SIZE) {
            commit();
        }
    }

    private void commit() {
        LOG.info("Commit start, inserted {} things", count);
        long time = System.nanoTime();
        currentTransaction.commit();
        LOG.info("Commit end, took {}s", (double)(System.nanoTime() - time) / 1_000_000_000.0);
        currentTransaction = session.transaction(Transaction.Type.WRITE);
        count = 0;
        attributeTypeCache.clear();
        entityTypeCache.clear();
        relationTypeCache.clear();
        attributeCache.clear();
    }

    private void flush() {
        commit();
    }

    public void execute() throws InvalidProtocolBufferException {
        currentTransaction = session.transaction(Transaction.Type.WRITE);

        MigrateProto.Item item;
        while ((item = read()) != null) {
            switch (item.getItemCase()) {
                case HEADER:
                    MigrateProto.Item.Header header = item.getHeader();
                    LOG.info("Importing {} from Grakn {} to {} in Grakn {}",
                            header.getOriginalKeyspace(),
                            header.getGraknVersion(),
                            session.keyspace().name(),
                            Version.VERSION);
                    break;
                case ATTRIBUTE:
                    insertAttribute(item.getAttribute());
                    break;
                case ENTITY:
                    insertEntity(item.getEntity());
                    break;
                case RELATION:
                    insertRelation(item.getRelation());
                    break;
                case CHECKSUMS:
                    checkChecksums(item.getChecksums());
                    break;
            }
        }

        insertAttributeKeyOwnerships();

        flush();

        LOG.info("Imported {} entities, {} attributes, {} relations ({} roles), {} ownerships",
                entityCount,
                attributeCount,
                relationCount,
                roleCount,
                ownershipCount);
    }

    private void check(List<String> errors, String name, long expected, long actual) {
        if (actual != expected) {
            errors.add(name + " count was incorrect, was " + actual + " but should be " + expected);
        }
    }

    private void checkChecksums(MigrateProto.Item.Checksums checksums) {
        List<String> errors = new ArrayList<>();

        check(errors, "Attribute", checksums.getAttributeCount(), attributeCount);
        check(errors, "Entity", checksums.getEntityCount(), entityCount);
        check(errors, "Relation", checksums.getRelationCount(), relationCount);
        check(errors, "Role", checksums.getRoleCount(), roleCount);
        check(errors, "Ownership", checksums.getOwnershipCount(), ownershipCount);

        if (errors.size() > 0) {
            throw new IllegalStateException(String.join("\n", errors));
        }
    }

    private void insertAttribute(MigrateProto.Item.Attribute attributeMessage) {
        Attribute<?> attribute = attributeTypeCache.computeIfAbsent(
                        attributeMessage.getLabel(),
                        l -> currentTransaction.getAttributeType(l))
                .create(valueFrom(attributeMessage.getValue()));

        String attributeId = attribute.id().toString();
        idMap.put(attributeMessage.getId(), attributeId);

        if (attributeMessage.getAttributeCount() > 0) {
            List<String> attributeIdStrings = new ArrayList<>(attributeMessage.getAttributeCount());

            for (MigrateProto.Item.OwnedAttribute ownedMessage : attributeMessage.getAttributeList()) {
                attributeIdStrings.add(ownedMessage.getId());
            }

            attributeOwnerships.add(new Pair<>(attributeId, attributeIdStrings));
        }

        attributeCount++;
        write();
    }

    private void insertAttributeKeyOwnerships() {
        for (Pair<String, List<String>> attributeKeyOwnership : attributeOwnerships) {
            Attribute<?> owner = attributeCache.computeIfAbsent(
                    attributeKeyOwnership.first(), // First is local ID
                    id -> currentTransaction.getConcept(ConceptId.of(id)));

            for (String attributeIdString : attributeKeyOwnership.second()) {
                Attribute<?> owned = attributeCache.computeIfAbsent(
                        idMap.get(attributeIdString), // Second is foreign ID, remember to map
                        id -> currentTransaction.getConcept(ConceptId.of(id)));
                owner.has(owned);
                ownershipCount++;
            }

            write();
        }

        attributeOwnerships.clear();
    }

    private void insertEntity(MigrateProto.Item.Entity entityMessage) {
        Entity entity = entityTypeCache.computeIfAbsent(
                        entityMessage.getLabel(),
                        l -> currentTransaction.getEntityType(l))
                .create();

        idMap.put(entityMessage.getId(), entity.id().toString());

        for (MigrateProto.Item.OwnedAttribute attributeMessage : entityMessage.getAttributeList()) {
            Attribute<?> attribute = attributeCache.computeIfAbsent(
                    idMap.get(attributeMessage.getId()),
                    id -> currentTransaction.getConcept(ConceptId.of(id)));
            entity.has(attribute);
            ownershipCount++;
        }

        entityCount++;
        write();
    }

    private void insertRelation(MigrateProto.Item.Relation relationMessage) {
        Relation relation = relationTypeCache.computeIfAbsent(
                        relationMessage.getLabel(),
                        l -> currentTransaction.getRelationType(l))
                .create();

        idMap.put(relationMessage.getId(), relation.id().toString());

        for (MigrateProto.Item.Relation.Role roleMessage : relationMessage.getRoleList()) {
            Role role = currentTransaction.getRole(roleMessage.getLabel());

            for (MigrateProto.Item.Relation.Role.Player playerMessage : roleMessage.getPlayerList()) {
                ConceptId localPlayerId = ConceptId.of(idMap.get(playerMessage.getId()));
                relation.assign(role, currentTransaction.getConcept(localPlayerId));
                roleCount++;
            }
        }

        for (MigrateProto.Item.OwnedAttribute attributeMessage : relationMessage.getAttributeList()) {
            ConceptId localAttributeId = ConceptId.of(idMap.get(attributeMessage.getId()));
            Attribute<?> attribute = currentTransaction.getConcept(localAttributeId);
            relation.has(attribute);
            ownershipCount++;
        }

        relationCount++;
        write();
    }

//    private void insertOwnership(MigrateProto.Item.Ownership ownership) {
//        ConceptId localOwnerId = ConceptId.of(idMap.get(ownership.getOwnerId()));
//        ConceptId localAttributeId = ConceptId.of(idMap.get(ownership.getAttributeId()));
//        Thing thing = currentTransaction.getConcept(localOwnerId);
//        Attribute<?> attribute = currentTransaction.getConcept(localAttributeId);
//        thing.has(attribute);
//
//        ownershipCount++;
//        write();
//    }

    private <T> T valueFrom(MigrateProto.ValueObject valueObject) {
        switch (valueObject.getValueCase()) {
            case STRING:
                return (T) valueObject.getString();
            case BOOLEAN:
                return (T) (Boolean) valueObject.getBoolean();
            case LONG:
                return (T) (Long) valueObject.getLong();
            case DOUBLE:
                return (T) (Double) valueObject.getDouble();
            case DATETIME:
                return (T) Instant.ofEpochMilli(valueObject.getDatetime()).atZone(ZoneId.of("Z")).toLocalDateTime();
            case VALUE_NOT_SET:
            default:
                throw new IllegalStateException("No value type was matched.");
        }
    }
}
