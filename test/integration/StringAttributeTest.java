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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static java.nio.charset.StandardCharsets.UTF_8;
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
            int end = 0x2FFFF; // unicode end is 0x10FFFF, however these are non-characters
            List<Pair<Integer, Integer>> excludes = list(pair(0xD800, 0xDFFF), pair(0xFDD0, 0xFDEF));
            Set<Integer> nonCharacter = set(0xFFFE , 0xFFFF, 0x1FFFE , 0x1FFFF, 0x2FFFE , 0x2FFFF,
                     0x3FFFE , 0x3FFFF, 0x4FFFE , 0x4FFFF, 0x5FFFE , 0x5FFFF, 0x6FFFE , 0x6FFFF, 0x7FFFE , 0x7FFFF,
                     0x8FFFE , 0x8FFFF, 0x9FFFE , 0x9FFFF, 0xaFFFE , 0xAFFFF, 0xbFFFE , 0xBFFFF, 0xcFFFE , 0xCFFFF,
                     0xdFFFE , 0xDFFFF, 0xeFFFE , 0xEFFFF, 0xfFFFE , 0xFFFFF, 0x10FFFE ,0x10FFFF);

            System.out.println("Code point range: " + (end - start));

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
                    for (int codePoint = 0; codePoint <= end; codePoint ++) {
                        if (!exclude(codePoint, excludes) && !nonCharacter.contains(codePoint)) {
                            if (codePoint % 1_000 == 0) System.out.println("Generating and inserting: " + codePoint);
                            // convert each code point into the string equivalent
                            String string = new String(new int[]{codePoint}, 0, 1);
                            generatedStrings.add(string);
                            attrType.put(string);
                        }
                    }
                    txn.commit();
                }

                // do a point-lookup for each generated string
                int success = 0;
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
//                        String escaped = generated.replace("\\", "\\\\").replace("\"", "\\\"");
                        if (!(generated.contains("\\") || generated.contains("\\\""))) {
                            Optional<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $a \"" + generated + "\" isa string-value;").asMatch()).first();
                            assertTrue(ans.isPresent());
                            assertEquals(generated, ans.get().get("a").asAttribute().asString().getValue());
                            success++;
                            System.out.println("success: " + success);
                        }
                    }
                }
            }
        }

    }

    private boolean exclude(int codePoint, List<Pair<Integer, Integer>> excludes) {
        for (Pair<Integer, Integer> range : excludes) {
            if (codePoint >= range.first() && codePoint < range.second()) return true;
        }
        return false;
    }

}