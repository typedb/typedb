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
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.reasoner.benchmark.synthetic.Util;

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph {
    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;

    private final static String schemaPath = "test/benchmark/reasoner/synthetic/resources/";
    private final String schemaFile;
    private final static Label key = Label.of("index");

    public PathTreeGraph(TypeDB.DatabaseManager databaseManager, String dbName, String schemaFile) {
        this.databaseManager = databaseManager;
        this.databaseName = dbName;
        this.schemaFile = schemaPath + schemaFile;
    }

    public PathTreeGraph(TypeDB.DatabaseManager databaseManager, String dbName) {
        this(databaseManager, dbName, "pathTest.tql");
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

    protected void buildExtensionalDB(int n, int children, TypeDB.Transaction tx) {
        buildTree("from", "to", n, children, tx);
    }

    void buildTree(String fromRoleValue, String toRoleValue, int n, int children, TypeDB.Transaction tx) {
        Label vertex = Label.of("vertex");
        Label startVertex = Label.of("start-vertex");

        RelationType arc = tx.concepts().getRelationType("arc");
        RoleType fromRole = arc.getRelates(fromRoleValue);
        RoleType toRole = arc.getRelates(toRoleValue);

        Thing a0 = Util.createEntityWithKey(tx, startVertex, key, "a0,0");

        int outputThreshold = 500;
        Thing[] prevLevel = new Thing[]{a0};
        Thing[] nextLevel;
        for (int i = 1; i <= n; i++) {
            nextLevel = new Thing[prevLevel.length * children];
            for (int j = 0; j < prevLevel.length; j++) {
                for (int c = 0; c < children; c++) {
                    int childIdx = (j * children + c);
                    nextLevel[childIdx] = Util.createEntityWithKey(tx, vertex, key, "a" + i + "," + childIdx);
                    Relation link = arc.create();
                    link.addPlayer(fromRole, prevLevel[j]);
                    link.addPlayer(toRole, nextLevel[childIdx]);
                }

                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println(j + " entities out of " + (prevLevel.length * children) + " inserted");
                }
            }
            prevLevel = nextLevel;
        }
    }
}
