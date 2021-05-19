/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.migrator;

import com.google.protobuf.Parser;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.migrator.proto.DataProto;
import com.vaticle.typedb.core.migrator.proto.MigratorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.FILE_NOT_READABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.INVALID_DATA;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Migrator.TYPE_NOT_FOUND;

public class DataImporter implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 20_000;
    private final TypeDB.Session session;
    private final Path filename;
    private final Map<String, String> remapLabels;

    private final Map<String, ByteArray> idMap = new HashMap<>();
    private final List<Pair<ByteArray, List<String>>> missingOwnerships = new ArrayList<>();
    private final List<Pair<ByteArray, List<Pair<String, List<String>>>>> missingRolePlayers = new ArrayList<>();
    private final String version;
    private long totalThingCount = 0;
    private long entityCount = 0;
    private long relationCount = 0;
    private long attributeCount = 0;
    private long ownershipCount = 0;
    private long playerCount = 0;
    private int txWriteCount = 0;
    private TypeDB.Transaction tx;

    DataImporter(TypeDB typedb, String database, Path filename, Map<String, String> remapLabels, String version) {
        if (!Files.exists(filename)) throw TypeDBException.of(FILE_NOT_FOUND, filename);
        this.session = typedb.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
        this.remapLabels = remapLabels;
        this.version = version;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        long current = attributeCount + relationCount + entityCount;
        return MigratorProto.Job.Progress.newBuilder()
                .setCurrent(current)
                .setTotal(Math.max(current, totalThingCount))
                .build();
    }

    @Override
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
            throw TypeDBException.of(FILE_NOT_READABLE, filename.toString());
        }

        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                switch (item.getItemCase()) {
                    case HEADER:
                        DataProto.Item.Header header = item.getHeader();
                        LOG.info("Importing {} from TypeDB {} to {} in TypeDB {}",
                                 header.getOriginalDatabase(),
                                 header.getTypedbVersion(),
                                 session.database().name(),
                                 version);
                        break;
                    case ENTITY:
                        insertEntity(item.getEntity());
                        break;
                    case RELATION:
                        insertRelation(item.getRelation());
                        break;
                    case ATTRIBUTE:
                        insertAttribute(item.getAttribute());
                        break;
                }
            }
        } catch (IOException e) {
            throw TypeDBException.of(FILE_NOT_READABLE, filename.toString());
        }

        insertMissingOwnerships();
        insertMissingRolePlayers();
        commit();
        tx.close();
        session.close();

        LOG.info("Imported {} entities, {} attributes, {} relations ({} players), {} ownerships",
                 entityCount,
                 attributeCount,
                 relationCount,
                 playerCount,
                 ownershipCount);
    }

    private void insertEntity(DataProto.Item.Entity entityMsg) {
        EntityType entityType = tx.concepts().getEntityType(relabel(entityMsg.getLabel()));
        if (entityType != null) {
            Entity entity = entityType.create();
            idMap.put(entityMsg.getId(), entity.getIID());
            insertOwnedAttributesThatExist(entity, entityMsg.getAttributeList());
            entityCount++;
            mayCommit();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(entityMsg.getLabel()), entityMsg.getLabel());
        }
    }

    private void insertRelation(DataProto.Item.Relation relationMsg) {
        RelationType relationType = tx.concepts().getRelationType(relabel(relationMsg.getLabel()));
        if (relationType != null) {
            Map<String, RoleType> roles = getScopedRoleTypes(relationType);
            Relation relation = relationType.create();
            idMap.put(relationMsg.getId(), relation.getIID());
            insertOwnedAttributesThatExist(relation, relationMsg.getAttributeList());

            List<Pair<String, List<String>>> missingRolePlayers = new ArrayList<>();
            for (DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                RoleType role = roles.get(relabel(roleMsg.getLabel()));
                if (role != null) {
                    List<String> missingPlayers = new ArrayList<>();
                    for (DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        Thing player = getThing(playerMessage.getId());
                        if (player != null) {
                            relation.addPlayer(role, player);
                            playerCount++;
                        } else {
                            missingPlayers.add(playerMessage.getId());
                        }
                    }
                    missingRolePlayers.add(new Pair<>(relabel(roleMsg.getLabel()), missingPlayers));
                } else {
                    throw TypeDBException.of(TYPE_NOT_FOUND, relabel(roleMsg.getLabel()), roleMsg.getLabel());
                }
            }
            this.missingRolePlayers.add(new Pair<>(relation.getIID(), missingRolePlayers));

            relationCount++;
            mayCommit();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(relationMsg.getLabel()), relationMsg.getLabel());
        }
    }

    private void insertAttribute(DataProto.Item.Attribute attributeMsg) {
        AttributeType attributeType = tx.concepts().getAttributeType(relabel(attributeMsg.getLabel()));
        if (attributeType != null) {
            DataProto.ValueObject valueMsg = attributeMsg.getValue();
            Attribute attribute;
            switch (valueMsg.getValueCase()) {
                case STRING:
                    attribute = attributeType.asString().put(valueMsg.getString());
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
                    throw TypeDBException.of(INVALID_DATA);
            }
            idMap.put(attributeMsg.getId(), attribute.getIID());
            insertOwnedAttributesThatExist(attribute, attributeMsg.getAttributeList());
            attributeCount++;
            mayCommit();
        } else {
            throw TypeDBException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
        }
    }

    private void insertOwnedAttributesThatExist(Thing thing, List<DataProto.Item.OwnedAttribute> ownedMsgs) {
        List<String> missingOwnerships = new ArrayList<>();
        for (DataProto.Item.OwnedAttribute ownedMsg : ownedMsgs) {
            Thing attrThing = getThing(ownedMsg.getId());
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

    private void insertMissingOwnerships() {
        for (Pair<ByteArray, List<String>> ownership : missingOwnerships) {
            Thing thing = tx.concepts().getThing(ownership.first());
            for (String originalAttributeId : ownership.second()) {
                Thing attrThing = getThing(originalAttributeId);
                assert thing != null && attrThing != null;
                thing.setHas(attrThing.asAttribute());
                ownershipCount++;
            }
            mayCommit();
        }
        missingOwnerships.clear();
    }

    private void insertMissingRolePlayers() {
        for (Pair<ByteArray, List<Pair<String, List<String>>>> rolePlayers : missingRolePlayers) {
            Thing thing = tx.concepts().getThing(rolePlayers.first());
            assert thing != null;
            Relation relation = thing.asRelation();
            for (Pair<String, List<String>> pair : rolePlayers.second()) {
                Map<String, RoleType> roles = getScopedRoleTypes(relation.getType());
                RoleType role = roles.get(pair.first());
                assert role != null;
                for (String originalPlayerId : pair.second()) {
                    Thing player = getThing(originalPlayerId);
                    relation.addPlayer(role, player);
                    playerCount++;
                }
            }
            mayCommit();
        }
        missingRolePlayers.clear();
    }

    private Thing getThing(String originalId) {
        ByteArray newId = idMap.get(originalId);
        return newId != null ? tx.concepts().getThing(newId) : null;
    }

    private Map<String, RoleType> getScopedRoleTypes(RelationType relationType) {
        return relationType.getRelates().stream().collect(
                Collectors.toMap(x -> x.getLabel().scopedName(), x -> x));
    }

    private String relabel(String label) {
        return remapLabels.getOrDefault(label, label);
    }

    private void mayCommit() {
        txWriteCount++;
        if (txWriteCount >= BATCH_SIZE) {
            commit();
        }
    }

    private void commit() {
        LOG.debug("Commit start, inserted {} things", txWriteCount);
        Instant start = Instant.now();
        tx.commit();
        LOG.debug("Commit end, took {}s", Duration.between(start, Instant.now()).toMillis());
        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        txWriteCount = 0;
    }
}
