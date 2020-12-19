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
import java.time.Duration;
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

public class Importer implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorRPCService.class);
    private static final Parser<DataProto.Item> ITEM_PARSER = DataProto.Item.parser();
    private static final int BATCH_SIZE = 20_000;
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
    private int txWriteCount = 0;
    private Grakn.Transaction tx;

    public Importer(final Grakn grakn, final String database, final Path filename, final Map<String, String> remapLabels) {
        this.session = grakn.session(database, Arguments.Session.Type.DATA);
        this.filename = filename;
        this.remapLabels = remapLabels;
    }

    @Override
    public MigratorProto.Job.Progress getProgress() {
        final long current = attributeCount + relationCount + entityCount;
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
        try (final InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                if (item.getItemCase() == DataProto.Item.ItemCase.ENTITY ||
                        item.getItemCase() == DataProto.Item.ItemCase.RELATION ||
                        item.getItemCase() == DataProto.Item.ItemCase.ATTRIBUTE) {
                    totalThingCount++;
                }
            }
        } catch (final IOException e) {
            throw GraknException.of(FILE_NOT_READABLE, filename.toString());
        }

        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        try (final InputStream inputStream = new BufferedInputStream(Files.newInputStream(filename))) {
            DataProto.Item item;
            while ((item = ITEM_PARSER.parseDelimitedFrom(inputStream)) != null) {
                switch (item.getItemCase()) {
                    case HEADER:
                        final DataProto.Item.Header header = item.getHeader();
                        LOG.info("Importing {} from Grakn {} to {} in Grakn {}",
                                header.getOriginalDatabase(),
                                header.getGraknVersion(),
                                session.database().name(),
                                Version.VERSION);
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
        } catch (final IOException e) {
            throw GraknException.of(FILE_NOT_READABLE, filename.toString());
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

    private void insertEntity(final DataProto.Item.Entity entityMsg) {
        final EntityType entityType = tx.concepts().getEntityType(relabel(entityMsg.getLabel()));
        if (entityType != null) {
            final Entity entity = entityType.create();
            idMap.put(entityMsg.getId(), entity.getIID());
            insertOwnedAttributesThatExist(entity, entityMsg.getAttributeList());
            entityCount++;
            mayCommit();
        } else {
            throw GraknException.of(TYPE_NOT_FOUND, relabel(entityMsg.getLabel()), entityMsg.getLabel());
        }
    }

    private void insertRelation(final DataProto.Item.Relation relationMsg) {
        final RelationType relationType = tx.concepts().getRelationType(relabel(relationMsg.getLabel()));
        if (relationType != null) {
            final Map<String, RoleType> roles = getScopedRoleTypes(relationType);
            final Relation relation = relationType.create();
            idMap.put(relationMsg.getId(), relation.getIID());
            insertOwnedAttributesThatExist(relation, relationMsg.getAttributeList());

            final List<Pair<String, List<String>>> missingRolePlayers = new ArrayList<>();
            for (final DataProto.Item.Relation.Role roleMsg : relationMsg.getRoleList()) {
                final RoleType role = roles.get(relabel(roleMsg.getLabel()));
                if (role != null) {
                    final List<String> missingPlayers = new ArrayList<>();
                    for (final DataProto.Item.Relation.Role.Player playerMessage : roleMsg.getPlayerList()) {
                        final Thing player = getThing(playerMessage.getId());
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

    private void insertAttribute(final DataProto.Item.Attribute attributeMsg) {
        final AttributeType attributeType = tx.concepts().getAttributeType(relabel(attributeMsg.getLabel()));
        if (attributeType != null) {
            final DataProto.ValueObject valueMsg = attributeMsg.getValue();
            final Attribute attribute;
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
                    throw GraknException.of(INVALID_DATA);
            }
            idMap.put(attributeMsg.getId(), attribute.getIID());
            insertOwnedAttributesThatExist(attribute, attributeMsg.getAttributeList());
            attributeCount++;
            mayCommit();
        } else {
            throw GraknException.of(TYPE_NOT_FOUND, relabel(attributeMsg.getLabel()), attributeMsg.getLabel());
        }
    }

    private void insertOwnedAttributesThatExist(final Thing thing, final List<DataProto.Item.OwnedAttribute> ownedMsgs) {
        final List<String> missingOwnerships = new ArrayList<>();
        for (final DataProto.Item.OwnedAttribute ownedMsg : ownedMsgs) {
            final Thing attrThing = getThing(ownedMsg.getId());
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
        for (final Pair<byte[], List<String>> ownership : missingOwnerships) {
            final Thing thing = tx.concepts().getThing(ownership.first());
            for (final String originalAttributeId : ownership.second()) {
                final Thing attrThing = getThing(originalAttributeId);
                assert thing != null && attrThing != null;
                thing.setHas(attrThing.asAttribute());
                ownershipCount++;
            }
            mayCommit();
        }
        missingOwnerships.clear();
    }

    private void insertMissingRolePlayers() {
        for (final Pair<byte[], List<Pair<String, List<String>>>> rolePlayers : missingRolePlayers) {
            final Thing thing = tx.concepts().getThing(rolePlayers.first());
            assert thing != null;
            final Relation relation = thing.asRelation();
            for (final Pair<String, List<String>> pair : rolePlayers.second()) {
                final Map<String, RoleType> roles = getScopedRoleTypes(relation.getType());
                final RoleType role = roles.get(pair.first());
                assert role != null;
                for (final String originalPlayerId : pair.second()) {
                    final Thing player = getThing(originalPlayerId);
                    relation.addPlayer(role, player);
                    playerCount++;
                }
            }
            mayCommit();
        }
        missingRolePlayers.clear();
    }

    private Thing getThing(final String originalId) {
        final byte[] newId = idMap.get(originalId);
        return newId != null ? tx.concepts().getThing(newId) : null;
    }

    private Map<String, RoleType> getScopedRoleTypes(final RelationType relationType) {
        return relationType.getRelates().collect(
                Collectors.toMap(x -> x.getLabel().scopedName(), x -> x));
    }

    private String relabel(final String label) {
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
        final Instant start = Instant.now();
        tx.commit();
        LOG.debug("Commit end, took {}s", Duration.between(start, Instant.now()).toMillis());
        tx = session.transaction(Arguments.Transaction.Type.WRITE);
        txWriteCount = 0;
    }
}
