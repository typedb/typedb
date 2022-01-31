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

package com.vaticle.typedb.core.graph;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreFactory;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.database.Factory;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLong;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLongAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeStringAsSorted;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ENTITY;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_RELATION;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ROLE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ENTITY;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.RELATION;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ROLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncodingTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("encoding-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static final String database = "encoding-test";

    private static final Factory factory = new CoreFactory();
    private static final CoreDatabaseManager dbMgr = factory.databaseManager(options);

    private CoreSession session;

    @BeforeClass
    public static void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        dbMgr.create(database);
        TypeDB.Session session = dbMgr.session(database, Arguments.Session.Type.SCHEMA);
        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            transaction.query().define(TypeQL.parseQuery("define " +
                    "person sub entity, owns name, plays employment:employee;" +
                    "company sub entity, owns company-id, plays employment:employer;" +
                    "employment sub relation, relates employer, relates employee;" +
                    "name sub attribute, value string;" +
                    "company-id sub attribute, value long;"));
            transaction.commit();
        }
        session.close();
    }

    @AfterClass
    public static void tearDown() {
        dbMgr.close();
    }

    @Before
    public void before() {
        session = dbMgr.session(database, Arguments.Session.Type.DATA);
    }

    @After
    public void after() {
        session.close();
    }

    private List<KeyValue<RawKey, ByteArray>> vertexElements(Storage.Data storage) {
        return storage.iterate(new RawKeyPrefix(ByteArray.empty(), Storage.Key.Partition.DEFAULT))
                .filter(kv -> !kv.key().bytes().hasPrefix(Encoding.Prefix.SYSTEM.bytes()))
                .toList();
    }

    private List<KeyValue<RawKey, ByteArray>> variableStartEdgeElements(Storage.Data storage) {
        return storage.iterate(new RawKeyPrefix(ByteArray.empty(), Storage.Key.Partition.VARIABLE_START_EDGE)).toList();
    }

    private List<KeyValue<RawKey, ByteArray>> fixedStartEdgeElements(Storage.Data storage) {
        return storage.iterate(new RawKeyPrefix(ByteArray.empty(), Storage.Key.Partition.FIXED_START_EDGE)).toList();
    }

    private List<KeyValue<RawKey, ByteArray>> optimisationEdgeElements(Storage.Data storage) {
        return storage.iterate(new RawKeyPrefix(ByteArray.empty(), Storage.Key.Partition.OPTIMISATION_EDGE)).toList();
    }

    private ByteArray expectedIID(GraphManager graphMgr, Label typeLabel, long instanceNumber) {
        TypeVertex type = graphMgr.schema().getType(typeLabel);
        if (type.isEntityType())
            return ByteArray.join(ENTITY.prefix().bytes(), type.iid().bytes(), encodeLongAsSorted(instanceNumber));
        else if (type.isRelationType())
            return ByteArray.join(RELATION.prefix().bytes(), type.iid().bytes(), encodeLongAsSorted(instanceNumber));
        else if (type.isRoleType())
            return ByteArray.join(ROLE.prefix().bytes(), type.iid().bytes(), encodeLongAsSorted(instanceNumber));
        else throw TypeDBException.of(ILLEGAL_ARGUMENT);
    }

    private ByteArray expectedAttributeIID(GraphManager graphMgr, Label attributeTypeLabel, String attributeValue) {
        TypeVertex type = graphMgr.schema().getType(attributeTypeLabel);
        assertTrue(type.isAttributeType());
        try {
            return ByteArray.join(ATTRIBUTE.prefix().bytes(), type.iid().bytes(), Encoding.ValueType.STRING.bytes(), encodeStringAsSorted(attributeValue, STRING_ENCODING));
        } catch (TypeDBCheckedException e) {
            throw TypeDBException.of(e);
        }
    }

    private ByteArray expectedAttributeIID(GraphManager graphMgr, Label attributeTypeLabel, long attributeValue) {
        TypeVertex type = graphMgr.schema().getType(attributeTypeLabel);
        assertTrue(type.isAttributeType());
        return ByteArray.join(ATTRIBUTE.prefix().bytes(), type.iid().bytes(), Encoding.ValueType.LONG.bytes(), encodeLongAsSorted(attributeValue));
    }

    @Test
    public void encoded_keys_are_correct() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            Storage.Data storage = transaction.traversal().graph().data().storage();
            assertEquals(0, vertexElements(storage).size());
            assertEquals(0, variableStartEdgeElements(storage).size());
            assertEquals(0, fixedStartEdgeElements(storage).size());
            assertEquals(0, optimisationEdgeElements(storage).size());

            transaction.query().insert(TypeQL.parseQuery("insert " +
                    "$p isa person, has name 'Alice';" +
                    "$c isa company, has company-id 10;" +
                    "(employer: $c, employee: $p) isa employment;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            GraphManager graph = transaction.traversal().graph();
            Storage.Data storage = graph.data().storage();
            List<KeyValue<RawKey, ByteArray>> vertexKVs = vertexElements(storage);
            assertTrue(iterate(vertexKVs).allMatch(kv -> kv.value().isEmpty()));
            List<ByteArray> vertices = iterate(vertexKVs).map(kv -> kv.key().bytes()).toList();

            // we must have exactly the expected set of vertex bytes
            assertEquals(7, vertices.size());
            ByteArray personIID = expectedIID(graph, Label.of("person"), 0);
            assertTrue(vertices.contains(personIID));
            ByteArray companyIID = expectedIID(graph, Label.of("company"), 0);
            assertTrue(vertices.contains(companyIID));
            ByteArray employmentIID = expectedIID(graph, Label.of("employment"), 0);
            assertTrue(vertices.contains(employmentIID));
            ByteArray employeeRoleIID = expectedIID(graph, Label.of("employee", "employment"), 0);
            assertTrue(vertices.contains(employeeRoleIID));
            ByteArray employerRoleIID = expectedIID(graph, Label.of("employer", "employment"), 0);
            assertTrue(vertices.contains(employerRoleIID));
            ByteArray nameAliceIID = expectedAttributeIID(graph, Label.of("name"), "Alice");
            assertTrue(vertices.contains(nameAliceIID));
            ByteArray companyId10IID = expectedAttributeIID(graph, Label.of("company-id"), 10);
            assertTrue(vertices.contains(companyId10IID));

            // we must have exactly the right set of fixed length edges: relates, plays, and forward has
            List<KeyValue<RawKey, ByteArray>> fixedKVs = fixedStartEdgeElements(storage);
            assertTrue(iterate(fixedKVs).allMatch(kv -> kv.value().isEmpty()));
            List<ByteArray> fixed = iterate(fixedKVs).map(kv -> kv.key().bytes()).toList();
            assertEquals(10, fixed.size());
            assertTrue(fixed.contains(join(personIID, HAS.forward().bytes(), nameAliceIID)));
            assertTrue(fixed.contains(join(personIID, PLAYING.forward().bytes(), employeeRoleIID)));
            assertTrue(fixed.contains(join(employeeRoleIID, PLAYING.backward().bytes(), personIID)));
            assertTrue(fixed.contains(join(employeeRoleIID, RELATING.backward().bytes(), employmentIID)));
            assertTrue(fixed.contains(join(employmentIID, RELATING.forward().bytes(), employeeRoleIID)));
            assertTrue(fixed.contains(join(employmentIID, RELATING.forward().bytes(), employerRoleIID)));
            assertTrue(fixed.contains(join(employerRoleIID, RELATING.backward().bytes(), employmentIID)));
            assertTrue(fixed.contains(join(employerRoleIID, PLAYING.backward().bytes(), companyIID)));
            assertTrue(fixed.contains(join(companyIID, PLAYING.forward().bytes(), employerRoleIID)));
            assertTrue(fixed.contains(join(companyIID, HAS.forward().bytes(), companyId10IID)));

            // we must have exactly the right set of variable length edges: backward has
            List<KeyValue<RawKey, ByteArray>> variableKVs = variableStartEdgeElements(storage);
            assertTrue(iterate(variableKVs).allMatch(kv -> kv.value().isEmpty()));
            List<ByteArray> variable = iterate(variableKVs).map(kv -> kv.key().bytes()).toList();
            assertEquals(2, variable.size());
            assertTrue(variable.contains(join(nameAliceIID, HAS.backward().bytes(), personIID)));
            assertTrue(variable.contains(join(companyId10IID, HAS.backward().bytes(), companyIID)));

            // we must have exactly the right set of optimisation edges: role player
            List<KeyValue<RawKey, ByteArray>> optimisationKVs = optimisationEdgeElements(storage);
            assertTrue(iterate(optimisationKVs).allMatch(kv -> kv.value().isEmpty()));
            List<ByteArray> optimisation = iterate(optimisationKVs).map(kv -> kv.key().bytes()).toList();
            assertEquals(4, optimisation.size());
            TypeVertex employerRoleType = graph.schema().getType(Label.of("employer", "employment"));
            TypeVertex employeeRoleType = graph.schema().getType(Label.of("employee", "employment"));
            assertTrue(optimisation.contains(join(personIID, ROLEPLAYER.backward().bytes(), employeeRoleType.iid().bytes(), employmentIID, employeeRoleIID)));
            assertTrue(optimisation.contains(join(employmentIID, ROLEPLAYER.forward().bytes(), employeeRoleType.iid().bytes(), personIID, employeeRoleIID)));
            assertTrue(optimisation.contains(join(companyIID, ROLEPLAYER.backward().bytes(), employerRoleType.iid().bytes(), employmentIID, employerRoleIID)));
            assertTrue(optimisation.contains(join(employmentIID, ROLEPLAYER.forward().bytes(), employerRoleType.iid().bytes(), companyIID, employerRoleIID)));
        }
    }

    private static class RawKey implements Storage.Key {

        private final ByteArray bytes;
        private final Partition partition;

        private RawKey(ByteArray bytes, Partition partition) {
            this.bytes = bytes;
            this.partition = partition;
        }

        @Override
        public ByteArray bytes() {
            return bytes;
        }

        @Override
        public Partition partition() {
            return partition;
        }
    }

    private static class RawKeyPrefix extends Storage.Key.Prefix<RawKey> {
        private RawKeyPrefix(ByteArray prefix, Partition partition) {
            super(prefix, partition, (key) -> new RawKey(key, partition));
        }
    }
}
