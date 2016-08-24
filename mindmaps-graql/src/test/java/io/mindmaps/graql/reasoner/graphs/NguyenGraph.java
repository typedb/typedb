package io.mindmaps.graql.reasoner.graphs;


import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;

public class NguyenGraph extends GenericGraph{

    public static MindmapsTransaction getTransaction(int n) {
        final String gqlFile = "nguyen-test.gql";
        getTransaction(gqlFile);
        buildExtensionalDB(n);
        return mindmaps;
    }

    private static void buildExtensionalDB(int n) {
        RoleType Rfrom = mindmaps.getRoleType("R-rA");
        RoleType Rto = mindmaps.getRoleType("R-rB");
        RoleType Qfrom = mindmaps.getRoleType("Q-rA");
        RoleType Qto = mindmaps.getRoleType("Q-rB");
        RoleType Pfrom = mindmaps.getRoleType("P-rA");
        RoleType Pto = mindmaps.getRoleType("P-rB");

        EntityType entity = mindmaps.getEntityType("entity");
        EntityType aEntity = mindmaps.getEntityType("a-entity");
        EntityType bEntity = mindmaps.getEntityType("b-entity");
        RelationType R = mindmaps.getRelationType("R");
        RelationType P = mindmaps.getRelationType("P");
        RelationType Q = mindmaps.getRelationType("Q");

        putEntity(aEntity, "a" + (n+1));
        for(int i = 0 ; i <= n ;i++) {
            putEntity(aEntity, "a" + i);
            putEntity(bEntity, "b" + i);
        }

        mindmaps.addRelation(R)
                .putRolePlayer(Rfrom, mindmaps.getInstance("d"))
                .putRolePlayer(Rto, mindmaps.getInstance("e"));

        mindmaps.addRelation(P)
                .putRolePlayer(Pfrom, mindmaps.getInstance("c"))
                .putRolePlayer(Pto, mindmaps.getInstance("d"));

        mindmaps.addRelation(Q)
                .putRolePlayer(Qfrom, mindmaps.getInstance("e"))
                .putRolePlayer(Qto, mindmaps.getInstance("a0"));

        for(int i = 0 ; i <= n ;i++){
            mindmaps.addRelation(P)
                    .putRolePlayer(Pfrom, mindmaps.getInstance("b" + i))
                    .putRolePlayer(Pto, mindmaps.getInstance("c"));
            mindmaps.addRelation(P)
                    .putRolePlayer(Pfrom, mindmaps.getInstance("c"))
                    .putRolePlayer(Pto, mindmaps.getInstance("b" + i));

            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance("a" + i))
                    .putRolePlayer(Qto, mindmaps.getInstance("b" + i));
            mindmaps.addRelation(Q)
                    .putRolePlayer(Qfrom, mindmaps.getInstance("b" + i))
                    .putRolePlayer(Qto, mindmaps.getInstance("a" + (i+1)));
        }

    }

    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }
}
