/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.benchmark.synthetic.generation;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.reasoner.benchmark.synthetic.Util;

@SuppressWarnings("CheckReturnValue")
public class DiagonalGraph {

    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;

    private static final String schemaFile = "test/benchmark/reasoner/synthetic/resources/diagonalTest.tql";
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
