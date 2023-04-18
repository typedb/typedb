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

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph {
    private final TypeDB.DatabaseManager databaseManager;
    private final String databaseName;

    private final static String schemaPath = "test/benchmark/reasoner/resources/";
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
