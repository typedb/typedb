package com.vaticle.typedb.core.reasoner.benchmark;


import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph{
    private final TypeDB.DatabaseManager dbm;
    private final String databaseName;

    private final static String schemaPath = "test/integration/reasoner/benchmark/resources/";
    private final String schemaFile;
    private final static Label key = Label.of("index");


    public PathTreeGraph(TypeDB.DatabaseManager dbm, String dbName, String schemaFile){
        this.dbm = dbm;
        this.databaseName = dbName;
        this.schemaFile = schemaPath + schemaFile;
    }

    public PathTreeGraph(TypeDB.DatabaseManager dbm, String dbName){
        this(dbm, dbName, "pathTest.tql");
    }

    public final void load(int n, int m) {
        try (TypeDB.Session session = dbm.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(Util.parseTQL(schemaFile).asDefine());
                tx.commit();
            }
        }

        try (TypeDB.Session session = dbm.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                buildExtensionalDB(n, m, tx);
                tx.commit();
            }
        }
    }

    protected void buildExtensionalDB(int n, int children, TypeDB.Transaction tx) {
        buildTree("from", "to", n , children, tx);
    }

    void buildTree(String fromRoleValue, String toRoleValue, int n, int children, TypeDB.Transaction tx) {
        EntityType vertex = tx.concepts().getEntityType("vertex");
        EntityType startVertex = tx.concepts().getEntityType("start-vertex");

        RelationType arc = tx.concepts().getRelationType("arc");
        RoleType fromRole = arc.getRelates(fromRoleValue);
        RoleType toRole = arc.getRelates(toRoleValue);

        Thing a0 = Util.putEntityWithResource(tx, "a0,0", startVertex, key);

        int outputThreshold = 500;



        Thing[] prevLevel = new Thing[] {a0};
        Thing[] nextLevel;
        for (int i = 1; i <= n; i++) {
            nextLevel = new Thing[prevLevel.length * children];
            for (int j = 0; j < prevLevel.length; j++) {
                for (int c = 0; c < children; c++) {
                    int childIdx = (j * children + c);
                    nextLevel[childIdx] = Util.putEntityWithResource(tx, "a" + i + "," + childIdx, vertex, key);
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