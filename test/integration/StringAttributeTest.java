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
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringAttributeTest {

    private static final String database = "string-attribute-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);

    @Test
    public void test_all_unicode_strings() throws IOException {
        Util.resetDirectory(dataDir);
        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            int start = 0x0;
            int end = 0x10FFFF; // unicode end is 0x10FFFF, however these are non-characters
            Pair<Integer, Integer> excludes = pair(0xD800, 0xDFFF); // since RFC 3629 (November 2003), this range (inclusive) is not valid unicode

            System.out.println("Number of code points: " + ((end - start) - (excludes.second() - excludes.first())));

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

                    for (int codePoint = 0; codePoint <= end; codePoint++) {
                        if (!exclude(codePoint, excludes)) {
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

                System.out.println("Generated all unicode characters.");

                // do a point-lookup for each generated string
                for (String generated : generatedStrings) {
                    // using Concept API
                    try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                        AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();
                        Attribute.String retrievedAttr = attrType.get(generated);
                        assertEquals(generated, retrievedAttr.getValue());
                    }
                    // using match query
                    try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                        // TODO escaping this doesn't work in TypeQL parser?
                        // String escaped = generated.replace("\\", "\\\\").replace("\"", "\\\"");
                        if (!(generated.contains("\\") || generated.contains("\""))) {
                            Optional<ConceptMap> ans = txn.query().match(TypeQL.parseQuery(
                                    "match $a \"" + generated + "\" isa string-value;").asMatch()).first();
                            assertTrue(ans.isPresent());
                            assertEquals(generated, ans.get().get("a").asAttribute().asString().getValue());
                        }
                    }
                }

                // iterate over all inserted strings and retrieve they are equal to the list of strings inserted
                try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    AttributeType.String attrType = txn.concepts().getAttributeType("string-value").asString();
                    List<String> strings = attrType.getInstances().map(a -> a.asString().getValue()).toList();
                    // because we retrieve them in sorted order, we can actually check the exact lists are equal!
                    assertEquals(generatedStrings, strings);
                }
            }
        }
    }

    private boolean exclude(int codePoint, Pair<Integer, Integer> excludes) {
        return codePoint >= excludes.first() && codePoint <= excludes.second();
    }
}