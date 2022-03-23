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

package com.vaticle.typedb.core.test.integration;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringAttributeTest {

    private static final String database = "string-attribute-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);

    // unicode defines an integer for each representable symbol in the range 0 to 0x10FFFF
    private static final int UNICODE_CODE_POINT_START = 0x0;
    private static final int UNICODE_CODE_POINT_END = 0x10FFFF;
    // since RFC 3629 (November 2003), this range (inclusive) is not valid unicode
    private static final Pair<Integer, Integer> UNICODE_INVALID_RANGE = pair(0xD800, 0xDFFF);

    @Test
    public void all_unicode_characters_are_valid() throws IOException {
        Util.resetDirectory(dataDir);
        try (TypeDB.DatabaseManager typedb = CoreDatabaseManager.open(options)) {
            typedb.create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.concepts().putAttributeType("string-value", AttributeType.ValueType.STRING);
                    txn.commit();
                }
            }

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                List<String> generatedStrings = new ArrayList<>();
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();

                    for (int codePoint = UNICODE_CODE_POINT_START; codePoint <= UNICODE_CODE_POINT_END; codePoint++) {
                        if (!isInRange(codePoint, UNICODE_INVALID_RANGE)) {
                            // convert each code point into the string equivalent
                            // for interest: in Java < 9, strings are UTF_16 encoded.
                            // in Java >= 9 they are "compact strings" which use 1 byte when possible
                            // otherwise reverts to UTF-16 which uses 2 byte/1 char as a code unit
                            String string = new String(new int[]{codePoint}, 0, 1);
                            generatedStrings.add(string);
                            attrType.put(string);
                        }
                    }
                    txn.commit();
                }

                // do a point-lookup for each generated string
                for (String generated : generatedStrings) {
                    // using Concept API, one tx per lookup to avoid any caching effects
                    try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                        AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();
                        Attribute.String retrievedAttr = attrType.get(generated);
                        assertEquals(generated, retrievedAttr.getValue());
                    }
                    // using match query, one tx per lookup to avoid any caching effects
                    try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                        // TODO escaping this doesn't work in TypeQL parser? It needs to remove the backslashes from parsed queries
                        // String escaped = generated.replace("\\", "\\\\").replace("\"", "\\\"");
                        if (!(generated.contains("\\") || generated.contains("\""))) {
                            Optional<ConceptMap> ans = txn.query().match(TypeQL.parseQuery(
                                    "match $a \"" + generated + "\" isa string-value;").asMatch()).first();
                            assertTrue(ans.isPresent());
                            assertEquals(generated, ans.get().get("a").asAttribute().asString().getValue());
                        }
                    }
                }

                // all strings stored in DB are correct
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();
                    Set<String> strings = attrType.getInstances().map(a -> a.asString().getValue()).toSet();
                    assertEquals(generatedStrings.size(), strings.size());
                    assertTrue(strings.containsAll(generatedStrings));
                }
            }
        }
    }

    @Test
    public void invalid_string_throws() throws IOException {
        Util.resetDirectory(dataDir);
        try (TypeDB.DatabaseManager typedb = CoreDatabaseManager.open(options)) {
            typedb.create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.concepts().putAttributeType("string-value", AttributeType.ValueType.STRING);
                    txn.commit();
                }
            }

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();
                    int unicodeExcludedCodePoint = UNICODE_INVALID_RANGE.first() + 1;
                    String excludedString = new String(new int[]{unicodeExcludedCodePoint}, 0, 1);
                    try {
                        attrType.put(excludedString);
                    } catch (TypeDBException e) {
                        assertTrue(e.code().isPresent());
                        assertEquals(ErrorMessage.ThingWrite.UNENCODABLE_STRING.code(), e.code().get());
                    }
                }
            }
        }
    }

    private boolean isInRange(int value, Pair<Integer, Integer> range) {
        return value >= range.first() && value <= range.second();
    }
}
