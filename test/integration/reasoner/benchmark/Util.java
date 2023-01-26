package com.vaticle.typedb.core.reasoner.benchmark;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Util {
    public static TypeQLQuery parseTQL(String tqlPath) throws IOException {
        return TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get(tqlPath))));
    }

    public static Thing putEntityWithResource(TypeDB.Transaction tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        Attribute attributeInstance = tx.concepts().getAttributeType(key.name()).asString().put(id);
        inst.setHas(attributeInstance);
        return inst;
    }
}
