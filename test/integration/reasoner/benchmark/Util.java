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
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Util {
    public static TypeQLQuery parseTQL(String tqlPath) {
        try {
            return TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get(tqlPath))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Thing putEntityWithResource(TypeDB.Transaction tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        Attribute attributeInstance = tx.concepts().getAttributeType(key.name()).asString().put(id);
        inst.setHas(attributeInstance);
        return inst;
    }
}
