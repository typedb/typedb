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

package com.vaticle.typedb.core.test.benchmark.generation;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.reasoner.benchmark.Util;

@SuppressWarnings("CheckReturnValue")
public class TransitivityChainGraph {

    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;

    private final static String schemaFile = "test/benchmark/reasoner/resources/quadraticTransitivity.tql";
    private final static Label key = Label.of("index");

    public TransitivityChainGraph(TypeDB.DatabaseManager databaseManager, String dbName) {
        this.databaseManager = databaseManager;
        this.databaseName = dbName;
    }

    public final void load(int n) {
        try (TypeDB.Session session = databaseManager.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(Util.parseTQL(schemaFile).asDefine());
                tx.commit();
            }
        }

        try (TypeDB.Session session = databaseManager.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                buildExtensionalDB(n, tx);
                tx.commit();
            }
        }
    }

    protected void buildExtensionalDB(int n, TypeDB.Transaction tx) {
        Label aEntity = Label.of("a-entity");
        RelationType Q = tx.concepts().getRelationType("Q");
        RoleType qfrom = Q.getRelates("from");
        RoleType qto = Q.getRelates("to");
        Thing aInst = Util.createEntityWithKey(tx, Label.of("entity2"), key, "a");
        Thing[] aInstanceIds = new Thing[n];
        for (int i = 0; i < n; i++) {
            aInstanceIds[i] = Util.createEntityWithKey(tx, aEntity, key, "a" + i);
        }

        Relation q0 = Q.create();
        q0.addPlayer(qfrom, aInst);
        q0.addPlayer(qto, aInstanceIds[0]);

        for (int i = 0; i < n - 1; i++) {
            Relation q = Q.create();
            q.addPlayer(qfrom, aInstanceIds[i]);
            q.addPlayer(qto, aInstanceIds[i + 1]);
        }
    }
}
