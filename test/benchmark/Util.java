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

package com.vaticle.typedb.core.reasoner.benchmark;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Util {
    public static TypeQLQuery parseTQL(String tqlPath) {
        try {
            return TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get(tqlPath))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Thing createEntityWithKey(TypeDB.Transaction tx, Label entityType, Label keyType, String key) {
        Thing inst = tx.concepts().getEntityType(entityType.name()).create();
        Attribute attributeInstance = tx.concepts().getAttributeType(keyType.name()).asString().put(key);
        inst.setHas(attributeInstance);
        return inst;
    }

    public static List<ConceptMap> timeQuery(String queryString, TypeDB.Transaction transaction, String msg) {
        return timeQuery(TypeQL.parseQuery(queryString).asMatch(), transaction, msg);
    }

    public static List<ConceptMap> timeQuery(TypeQLMatch query, TypeDB.Transaction transaction, String msg) {
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = (List<ConceptMap>) transaction.query().match(query).toList();
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}
