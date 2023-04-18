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
public abstract class TransitivityMatrixGraph {

    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;
    private final String schemaFile;

    private final static Label key = Label.of("index");

    public TransitivityMatrixGraph(String schemaFile, TypeDB.DatabaseManager databaseManager, String dbName) {
        this.databaseManager = databaseManager;
        this.databaseName = dbName;
        this.schemaFile = schemaFile;
    }

    public final void load(int n, int m) {
        try (TypeDB.Session session = databaseManager.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(Util.parseTQL(schemaFile).asDefine());
                tx.commit();
            }
        }

        try (TypeDB.Session session = databaseManager.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                buildExtensionalDB(n, m, tx);
                tx.commit();
            }
        }
    }

    protected void buildExtensionalDB(int n, int m, TypeDB.Transaction tx) {

        Label aEntity = Label.of("a-entity");
        RelationType Q = tx.concepts().getRelationType("Q");
        RoleType Qfrom = Q.getRelates("from");
        RoleType Qto = Q.getRelates("to");

        Thing[][] aInstancesIds = new Thing[n + 1][m + 1];
        Thing aInst = Util.createEntityWithKey(tx, Label.of("entity2"), key, "a");
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                aInstancesIds[i][j] = Util.createEntityWithKey(tx, aEntity, key, "a" + i + "," + j);
            }
        }

        Relation rel = Q.create();
        rel.addPlayer(Qfrom, aInst);
        rel.addPlayer(Qto, aInstancesIds[1][1]);

        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                if (i < n) {
                    Relation q = Q.create();
                    q.addPlayer(Qfrom, aInstancesIds[i][j]);
                    q.addPlayer(Qto, aInstancesIds[i][j]);
                }
                if (j < m) {
                    Relation q = Q.create();
                    q.addPlayer(Qfrom, aInstancesIds[i][j]);
                    q.addPlayer(Qto, aInstancesIds[i][j + 1]);
                }
            }
        }
    }

    public static class Linear extends TransitivityMatrixGraph {
        private static final String schemaFile = "test/benchmark/reasoner/resources/linearTransitivity.tql";

        public Linear(TypeDB.DatabaseManager dbm, String dbName) {
            super(schemaFile, dbm, dbName);
        }
    }

    public static class Quadratic extends TransitivityMatrixGraph {
        private static final String schemaFile = "test/benchmark/reasoner/resources/linearTransitivity.tql";

        public Quadratic(TypeDB.DatabaseManager dbm, String dbName) {
            super(schemaFile, dbm, dbName);
        }
    }
}
