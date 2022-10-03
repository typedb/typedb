/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.encoding.Storage;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.test.integration.util.Util;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Order.Desc.DESC;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.READ;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static org.junit.Assert.assertEquals;

public class RocksIteratorTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("iterator-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static final String database = "iterator-test";

    private static final Factory factory = new CoreFactory();
    private static final CoreDatabaseManager dbMgr = factory.databaseManager(options);

    private CoreSession session;

    @BeforeClass
    public static void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        dbMgr.create(database);
        TypeDB.Session session = dbMgr.session(database, Arguments.Session.Type.SCHEMA);
        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            transaction.concepts().putAttributeType("string-value", AttributeType.ValueType.STRING);
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

    @Test
    public void attributesRetrievedAscending() {
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            strings.add(UUID.randomUUID().toString());
        }

        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            AttributeType.String stringValueType = transaction.concepts().getAttributeType("string-value").asString();
            for (String string : strings) {
                stringValueType.put(string);
            }
            transaction.commit();
        }

        // test ascending order
        strings.sort(Comparator.naturalOrder());
        try (CoreTransaction transaction = session.transaction(READ)) {
            Storage.Data storage = transaction.graphMgr.data().storage();
            AttributeType.String stringValueType = transaction.concepts().getAttributeType("string-value").asString();
            VertexIID.Type iid = ((AttributeTypeImpl) stringValueType).vertex.iid();
            Key.Prefix<VertexIID.Thing> iteratePrefix = VertexIID.Thing.Attribute.String.prefix(iid);
            List<String> values = storage.iterate(iteratePrefix, ASC)
                    .map(kv -> kv.key().asAttribute().asString().value()).toList();
            assertEquals(strings, values);
        }

        // test descending order
        strings.sort(Comparator.reverseOrder());
        try (CoreTransaction transaction = session.transaction(READ)) {
            Storage.Data storage = transaction.graphMgr.data().storage();
            AttributeType.String stringValueType = transaction.concepts().getAttributeType("string-value").asString();
            VertexIID.Type iid = ((AttributeTypeImpl) stringValueType).vertex.iid();
            Key.Prefix<VertexIID.Thing> iteratePrefix = VertexIID.Thing.Attribute.String.prefix(iid);
            List<String> values = storage.iterate(iteratePrefix, DESC)
                    .map(kv -> kv.key().asAttribute().asString().value()).toList();
            assertEquals(strings, values);
        }
    }
}
