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
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.server.Version;
import grakn.core.server.migrate.proto.DataProto;
import grakn.core.server.session.TransactionImpl;
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
    private final Parser<DataProto.Item> itemParser;

    private Transaction currentTransaction;
    private int count = 0;

    Map<String, String> idMap = new HashMap<>();

    // Pair of IMPORTED thing ids to list of ORIGINAL attribute ids
    List<Pair<String, List<String>>> missingAttributeOwnerships = new ArrayList<>();

    // Pair of IMPORTED relation ids to map of role LABEL to lists of ORIGINAL player ids
    List<Pair<String, List<Pair<String, List<String>>>>> missingRolePlayers = new ArrayList<>();

    Map<String, AttributeType<?>> attributeTypeCache = new HashMap<>();
    Map<String, EntityType> entityTypeCache = new HashMap<>();
    Map<String, RelationType> relationTypeCache = new HashMap<>();
    Map<String, Role> roleCache = new HashMap<>();

    Map<String, Thing> thingCache = new HashMap<>();

    private long entityCount = 0;
    private long attributeCount = 0;
    private long relationCount = 0;
    private long ownershipCount = 0;
    private long roleCount = 0;

    public Import(Session session, Path inputFile) throws IOException {
        this.session = session;

        this.inputStream = new BufferedInputStream(Files.newInputStream(inputFile));
        this.itemParser = DataProto.Item.parser();
    }

    @Override
    public void close() throws IOException {
        if (currentTransaction != null) currentTransaction.close();
        inputStream.close();
    }

    private DataProto.Item read() throws InvalidProtocolBufferException {
        return itemParser.parseDelimitedFrom(inputStream);
    }

    private void write() {
        count++;
        if (count >= BATCH_SIZE) {
            commit();
        }
    }

    private void newTransaction() {
        currentTransaction = session.transaction(Transaction.Type.WRITE);
        ((TransactionImpl) currentTransaction).disableCommitValidation();
    }

    private void commit() {
        LOG.debug("Commit start, inserted {} things", count);
        long time = System.nanoTime();
        currentTransaction.commit();
        LOG.debug("Commit end, took {}s", (double)(System.nanoTime() - time) / 1_000_000_000.0);
        newTransaction();
        count = 0;
        attributeTypeCache.clear();
        entityTypeCache.clear();
        relationTypeCache.clear();
        roleCache.clear();
        thingCache.clear();
    }

    private void flush() {
        commit();
    }

    public void execute() throws InvalidProtocolBufferException {
        newTransaction();

        DataProto.Item item;
        DataProto.Item.Checksums checksums = null;
        while ((item = read()) != null) {
            switch (item.getItemCase()) {
                case HEADER:
                    DataProto.Item.Header header = item.getHeader();
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
                    checksums = item.getChecksums();
                    break;
            }
        }

        insertMissingRolePlayersAtEnd();
        insertMissingAttributeOwnershipsAtEnd();

        flush();

        if (checksums != null) {
            checkChecksums(checksums);
        }

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

    private void checkChecksums(DataProto.Item.Checksums checksums) {
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

    private void insertAttribute(DataProto.Item.Attribute attributeMessage) {
        Attribute<?> attribute = attributeTypeCache.computeIfAbsent(
                        attributeMessage.getLabel(),
                        l -> currentTransaction.getAttributeType(l))
                .create(valueFrom(attributeMessage.getValue()));

        String originalId = attributeMessage.getId();
        String attributeId = attribute.id().toString();
        idMap.put(originalId, attributeId);
        thingCache.put(originalId, attribute);

        insertOwnedAttributesThatExist(attribute, attributeMessage.getAttributeList());

        attributeCount++;
        write();
    }

    private void insertEntity(DataProto.Item.Entity entityMessage) {
        Entity entity = entityTypeCache.computeIfAbsent(
                        entityMessage.getLabel(),
                        l -> currentTransaction.getEntityType(l))
                .create();

        String originalId = entityMessage.getId();
        idMap.put(entityMessage.getId(), entity.id().toString());
        thingCache.put(originalId, entity);

        insertOwnedAttributesThatExist(entity, entityMessage.getAttributeList());

        entityCount++;
        write();
    }

    private void insertRelation(DataProto.Item.Relation relationMessage) {
        Relation relation = relationTypeCache.computeIfAbsent(
                        relationMessage.getLabel(),
                        l -> currentTransaction.getRelationType(l))
                .create();

        String originalId = relationMessage.getId();
        String importedId = relation.id().toString();
        idMap.put(originalId, importedId);
        thingCache.put(originalId, relation);

        List<Pair<String, List<String>>> missingRolePlayers = new ArrayList<>();

        for (DataProto.Item.Relation.Role roleMessage : relationMessage.getRoleList()) {
            List<String> missingPlayers = new ArrayList<>();
            Role role = null;

            for (DataProto.Item.Relation.Role.Player playerMessage : roleMessage.getPlayerList()) {
                String originalPlayerId = playerMessage.getId();
                Thing player = thingCache.get(originalPlayerId);
                if (player == null) {
                    String importedPlayerId = idMap.get(originalPlayerId);
                    if (importedPlayerId != null) {
                        player = currentTransaction.getConcept(ConceptId.of(importedPlayerId));
                        thingCache.put(originalPlayerId, player);
                    }
                }

                if (player != null) {
                    if (role == null) {
                        role = roleCache.computeIfAbsent(roleMessage.getLabel(), l -> currentTransaction.getRole(l));
                    }
                    relation.assign(role, player);
                    roleCount++;
                } else {
                    missingPlayers.add(originalPlayerId);
                }
            }

            if (!missingPlayers.isEmpty()) {
                missingRolePlayers.add(new Pair<>(roleMessage.getLabel(), missingPlayers));
            }
        }

        if (!missingRolePlayers.isEmpty()) {
            this.missingRolePlayers.add(new Pair<>(importedId, missingRolePlayers));
        }

        insertOwnedAttributesThatExist(relation, relationMessage.getAttributeList());

        relationCount++;
        write();
    }

    private void insertOwnedAttributesThatExist(Thing thing, List<DataProto.Item.OwnedAttribute> owned) {
        List<String> missingOwnerships = new ArrayList<>();

        for (DataProto.Item.OwnedAttribute ownedMessage : owned) {
            String ownedOriginalId = ownedMessage.getId();
            Attribute<?> ownedAttribute = (Attribute<?>) thingCache.get(ownedOriginalId);
            if (ownedAttribute == null) {
                String ownedNewId = idMap.get(ownedOriginalId);
                if (ownedNewId != null) {
                    ownedAttribute = currentTransaction.getConcept(ConceptId.of(ownedNewId));
                    thingCache.put(ownedOriginalId, ownedAttribute);
                }
            }

            if (ownedAttribute != null) {
                thing.has(ownedAttribute);
                ownershipCount++;
            } else {
                missingOwnerships.add(ownedMessage.getId());
            }
        }

        if (!missingOwnerships.isEmpty()) {
            missingAttributeOwnerships.add(new Pair<>(thing.id().toString(), missingOwnerships));
        }
    }

    private void insertMissingAttributeOwnershipsAtEnd() {
        for (Pair<String, List<String>> attributeKeyOwnership : missingAttributeOwnerships) {
            Attribute<?> owner = currentTransaction.getConcept(ConceptId.of(attributeKeyOwnership.first()));

            for (String attributeIdString : attributeKeyOwnership.second()) {
                Attribute<?> owned = (Attribute<?>) thingCache.computeIfAbsent(
                        idMap.get(attributeIdString), // Second is foreign ID, remember to map
                        id -> currentTransaction.getConcept(ConceptId.of(id)));
                owner.has(owned);
                ownershipCount++;
            }

            write();
        }

        missingAttributeOwnerships.clear();
    }

    private void insertMissingRolePlayersAtEnd() {
        for (Pair<String, List<Pair<String, List<String>>>> missingRolePlayers : missingRolePlayers) {
            Relation relation = currentTransaction.getConcept(ConceptId.of(missingRolePlayers.first()));

            for (Pair<String, List<String>> pair : missingRolePlayers.second()) {
                Role role = roleCache.computeIfAbsent(pair.first(), l -> currentTransaction.getRole(l));

                for (String playerOriginalId : pair.second()) {
                    Thing player = thingCache.computeIfAbsent(playerOriginalId,
                            poi -> currentTransaction.getConcept(ConceptId.of(idMap.get(poi))));
                    relation.assign(role, player);
                    roleCount++;
                }
            }

            write();
        }

        missingRolePlayers.clear();
    }

    private <T> T valueFrom(DataProto.ValueObject valueObject) {
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
