package io.mindmaps.graql.reasoner.graphs;


import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;

public class PathGraph extends GenericGraph {

    public static MindmapsTransaction getTransaction(int n, int m) {
        final String gqlFile = "path-test.gql";
        getTransaction(gqlFile);
        buildExtensionalDB(n, m);
        return mindmaps;
    }

    private static void buildExtensionalDB(int n, int m) {
        EntityType vertex = mindmaps.getEntityType("vertex");
        RoleType arcFrom = mindmaps.getRoleType("arc-from");
        RoleType arcTo = mindmaps.getRoleType("arc-to");

        RelationType arc = mindmaps.getRelationType("arc");
        putEntity(vertex, "a0");

        for(int i = 0 ; i < n ;i++)
            for(int j = 0; j < m; j++)
                putEntity(vertex, "a" + i + j);

        mindmaps.addRelation(arc)
                .putRolePlayer(arcFrom, mindmaps.getInstance("a0"))
                .putRolePlayer(arcTo, mindmaps.getInstance("a00"));

        for(int i = 0 ; i < n ;i++)
            for(int j = 0; j < m; j++) {
                if ( j < n -1 )
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, mindmaps.getInstance("a" + i + j))
                            .putRolePlayer(arcTo, mindmaps.getInstance("a" + i + (j + 1)));
                if (i < m - 1)
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, mindmaps.getInstance("a" + i + j))
                            .putRolePlayer(arcTo, mindmaps.getInstance("a" + (i+1) + j));
            }

    }

    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }
}
