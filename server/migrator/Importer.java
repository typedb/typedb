package grakn.core.server.migrator;

import com.google.protobuf.Parser;
import grakn.common.collection.Pair;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.server.Version;
import grakn.core.server.migrator.proto.DataProto;
import grakn.core.server.migrator.proto.MigratorProto;
import grakn.core.server.rpc.MigratorRPCService;
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
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Migrator.FILE_NOT_READABLE;
import static grakn.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static grakn.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;

public class Importer {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorRPCService.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 10_000;
    private final Grakn.Session session;
    private final Path filename;
    private final Map<String, String> remapLabels;

    private final Map<String, byte[]> idMap = new HashMap<>();
    private final List<Pair<byte[], List<String>>> missingOwnerships = new ArrayList<>();
    private final List<Pair<byte[], List<Pair<String, List<String>>>>> missingRolePlayers = new ArrayList<>();
    private long totalThingCount = 0;
    private long entityCount = 0;
    private long relationCount = 0;
    private long attributeCount = 0;
    private long ownershipCount = 0;
    private long playerCount = 0;
    private Grakn.Transaction tx;
    private int commitWriteCount = 0;

    public Importer(Grakn grakn, String database, Path filename, Map<String, String> remapLabels) {
        this.session = grakn.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
        this.remapLabels = remapLabels;
    }

    public MigratorProto.Job.Progress getProgress() {
        long current = attributeCount + relationCount + entityCount;
        return MigratorProto.Job.Progress.newBuilder()
                .setCurrent(current)
                .setTotal(Math.max(current, totalThingCount))
                .build();
    }

    public void run() {
        // We scan the file to find the checksum. This is probably not a good idea for files larger than several
        // gigabytes but that case is rare and the actual import would take so long that even if this took a few
        // seconds it would still be cheap.
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.ENTITY ||
                        item.getItemCase() == DataProto.Item.ItemCase.RELATION ||
                        item.getItemCase() == DataProto.Item.ItemCase.ATTRIBUTE) {
                    totalThingCount++;
                }
            }
        } catch (IOException e) {
            throw GraknException.of(FILE_NOT_READABLE, filename.toString());
        }

        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                switch (item.getItemCase()) {
                    case HEADER:
                        DataProto.Item.Header header = item.getHeader();
                        LOG.info("Importing {} from Grakn {} to {} in Grakn {}",
                                header.getOriginalDatabase(),
                                header.getGraknVersion(),
                                session.database().name(),
                                Version.VERSION);
                        break;
                    case ENTITY:
                        insertEntity(tx, item.getEntity());
                        break;
                    case RELATION:
                        insertRelation(tx, item.getRelation());
                        break;
                    case ATTRIBUTE:
                        insertAttribute(tx, item.getAttribute());
                        break;
                }
            }
        } catch (IOException e) {
            throw GraknException.of(FILE_NOT_READABLE, filename.toString());
        }

        insertMissingOwnerships(tx);
        insertMissingRolePlayers(tx);
        commit();

        LOG.info("Imported {} entities, {} attributes, {} relations ({} players), {} ownerships",
                entityCount,
                attributeCount,
                relationCount,
                playerCount,
                ownershipCount);
    }

    private void insertEntity(Grakn.Transaction tx, DataProto.Item.Entity entityMsg) {
        EntityType entityType = tx.concepts().getEntityType(relabel(entityMsg.getLabel()));
        if (entityType != null) {
            Entity entity = entityType.create();
            idMap.put(entityMsg.getId(), entity.getIID());
            insertOwnedAttributesThatExist(tx, entity, entityMsg.getAttributeList());
            entityCount++;
            mayCommit();
        } else {
            throw GraknException.of(TYPE_NOT_FOUND, relabel(entityMsg.getLabel()), entityMsg.getLabel());
        }
    }

    private void insertRelation(Grakn.Transaction tx, DataProto.Item.Relation relationMsg) {
        RelationType relationType = tx.concepts().getRelationType(relabel(relationMsg.getLabel()));
        if (relationType != null) {
            Map<String, RoleType> roles = getScopedRoleTypes(relationType);
            Relation relation = relationType.create();
            idMap.put(relationMsg.getId(), relation.getIID());
            insertOwnedAttributesThatExist(tx, relation, relationMsg.getAttributeList());

            List<Pair<String, List<String>>> missingRolePlayers = new ArrayList<>();
            for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                RoleType role = roles.get(relabel(roleMsg.getLabel()));
                if (role != null) {
                    List<String> missingPlayers = new ArrayList<>();
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = getThing(tx, playerMessage.getId());
                        if (player != null) {
                            relation.addPlayer(role, player);
                            playerCount++;
                        } else {
                            missingPlayers.add(playerMessage.getId());
                        }
                    }
                    missingRolePlayers.add(new Pair<>(relabel(roleMsg.getLabel()), missingPlayers));
                } else {
                    throw GraknException.of(TYPE_NOT_FOUND, relabel(roleMsg.getLabel()), roleMsg.getLabel());
                }
            }
            this.missingRolePlayers.add(new Pair<>(relation.getIID(), missingRolePlayers));

            relationCount++;
            mayCommit();
        } else {
            throw GraknException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
        }
    }

    private void insertAttribute(Grakn.Transaction tx, DataProto.Item.Attribute attributeMsg) {
        AttributeType attributeType = tx.concepts().getAttributeType(relabel(attributeMsg.getLabel()));
        if (attributeType != null) {
            DataProto.ValueObject valueMsg = attributeMsg.getValue();
            Attribute attribute;
            switch (valueMsg.getValueCase()) {
                case STRING:
                    attribute = attributeType.asString().put(valueMsg.getString().substring(0, Math.min(valueMsg.getString().length(), 250)));
                    break;
                case BOOLEAN:
                    attribute = attributeType.asBoolean().put(valueMsg.getBoolean());
                    break;
                case LONG:
                    attribute = attributeType.asLong().put(valueMsg.getLong());
                    break;
                case DOUBLE:
                    attribute = attributeType.asDouble().put(valueMsg.getDouble());
                    break;
                case DATETIME:
                    attribute = attributeType.asDateTime().put(
                            Instant.ofEpochMilli(valueMsg.getDatetime()).atZone(ZoneId.of("Z")).toLocalDateTime());
                    break;
                default:
                    throw GraknException.of(INVALID_DATA);
            }
            idMap.put(attributeMsg.getId(), attribute.getIID());
            insertOwnedAttributesThatExist(tx, attribute, attributeMsg.getAttributeList());
            attributeCount++;
            mayCommit();
        } else {
            throw GraknException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
        }
    }

    private void insertOwnedAttributesThatExist(Grakn.Transaction tx, Thing thing, List<DataProto.Item.OwnedAttribute> ownedMsgs) {
        List<String> missingOwnerships = new ArrayList<>();
        for (DataProto.Item.OwnedAttribute ownedMsg : ownedMsgs) {
            Thing attrThing = getThing(tx, ownedMsg.getId());
            if (attrThing != null) {
                thing.setHas(attrThing.asAttribute());
                ownershipCount++;
            } else {
                missingOwnerships.add(ownedMsg.getId());
            }
            mayCommit();
        }
        this.missingOwnerships.add(new Pair<>(thing.getIID(), missingOwnerships));
    }

    private void insertMissingOwnerships(Grakn.Transaction tx) {
        for (Pair<byte[], List<String>> ownership : missingOwnerships) {
            Thing thing = tx.concepts().getThing(ownership.first());
            for (String originalAttributeId : ownership.second()) {
                Thing attrThing = getThing(tx, originalAttributeId);
                assert thing != null && attrThing != null;
                thing.setHas(attrThing.asAttribute());
                ownershipCount++;
            }
            mayCommit();
        }
        missingOwnerships.clear();
    }

    private void insertMissingRolePlayers(Grakn.Transaction tx) {
        for (Pair<byte[], List<Pair<String, List<String>>>> rolePlayers : missingRolePlayers) {
            Thing thing = tx.concepts().getThing(rolePlayers.first());
            assert thing != null;
            Relation relation = thing.asRelation();
            for (Pair<String, List<String>> pair : rolePlayers.second()) {
                Map<String, RoleType> roles = getScopedRoleTypes(relation.getType());
                RoleType role = roles.get(pair.first());
                assert role != null;
                for (String originalPlayerId : pair.second()) {
                    Thing player = getThing(tx, originalPlayerId);
                    relation.addPlayer(role, player);
                    playerCount++;
                }
            }
            mayCommit();
        }
        missingRolePlayers.clear();
    }

    private Thing getThing(Grakn.Transaction tx, String originalId) {
        byte[] newId = idMap.get(originalId);
        return newId != null ? tx.concepts().getThing(newId) : null;
    }

    private Map<String, RoleType> getScopedRoleTypes(RelationType relationType) {
        return relationType.getRelates().collect(
                Collectors.toMap(x -> relationType.getLabel() + ":" + x.getLabel(), x -> x));
    }

    private String relabel(String label) {
        return remapLabels.getOrDefault(label, label);
    }

    private void mayCommit() {
        commitWriteCount++;
        if (commitWriteCount >= BATCH_SIZE) {
            commit();
        }
    }

    private void commit() {
        LOG.debug("Commit start, inserted {} things", commitWriteCount);
        long time = System.nanoTime();
        tx.commit();
        LOG.debug("Commit end, took {}s", (double)(System.nanoTime() - time) / 1_000_000_000.0);
        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        commitWriteCount = 0;
    }
}
