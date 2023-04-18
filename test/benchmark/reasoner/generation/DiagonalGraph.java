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
import com.vaticle.typedb.core.reasoner.benchmark.Util;

@SuppressWarnings("CheckReturnValue")
public class DiagonalGraph {

    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;

    private static final String schemaFile = "test/benchmark/reasoner/resources/diagonalTest.tql";
    private static final Label key = Label.of("name");

    public DiagonalGraph(TypeDB.DatabaseManager databaseManager, String dbName) {
        this.databaseManager = databaseManager;
        this.databaseName = dbName;
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

        Label entityType = Label.of("entity1");
        RelationType horizontal = tx.concepts().getRelationType("horizontal");
        RelationType vertical = tx.concepts().getRelationType("vertical");
        Thing[][] instanceIds = new Thing[n][m];
        long inserts = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                instanceIds[i][j] = Util.createEntityWithKey(tx, entityType, key, "a" + i + "," + j);
                inserts++;
                if (inserts % 100 == 0) System.out.println("inst inserts: " + inserts);
            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (i < n - 1) {
                    Relation v = vertical.create();
                    v.addPlayer(vertical.getRelates("from"), instanceIds[i][j]);
                    v.addPlayer(vertical.getRelates("to"), instanceIds[i + 1][j]);
                    inserts++;
                }
                if (j < m - 1) {
                    Relation h = horizontal.create();
                    h.addPlayer(horizontal.getRelates("from"), instanceIds[i][j]);
                    h.addPlayer(horizontal.getRelates("to"), instanceIds[i][j + 1]);
                    inserts++;
                }
                if (inserts % 100 == 0) System.out.println("rel inserts: " + inserts);
            }
        }
    }
}
