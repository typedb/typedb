/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.benchmark.synthetic;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLGet;
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
        return timeQuery(TypeQL.parseQuery(queryString).asGet(), transaction, msg);
    }

    public static List<ConceptMap> timeQuery(TypeQLGet query, TypeDB.Transaction transaction, String msg) {
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = (List<ConceptMap>) transaction.query().get(query).toList();
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }
}
