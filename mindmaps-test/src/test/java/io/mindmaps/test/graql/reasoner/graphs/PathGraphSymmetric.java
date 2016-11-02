package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;

import static com.google.common.math.IntMath.pow;

public class PathGraphSymmetric extends PathGraph{

    public static MindmapsGraph getGraph(int n, int children) {
        final String gqlFile = "path-test-symmetric.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return mindmaps;
    }

    protected static void buildExtensionalDB(int n, int children) {
        long startTime = System.currentTimeMillis();

        EntityType vertex = mindmaps.getEntityType("vertex");
        EntityType startVertex = mindmaps.getEntityType("start-vertex");
        RoleType arcFrom = mindmaps.getRoleType("arcA");
        RoleType arcTo = mindmaps.getRoleType("arcB");

        RelationType arc = mindmaps.getRelationType("arc");
        mindmaps.putEntity("a0", startVertex);

        for(int i = 1 ; i <= n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                mindmaps.putEntity("a" + i + "," + j, vertex);
                if (j != 0 && j % 100 ==0)
                    System.out.println(j + " entities out of " + m + " inserted");
            }
        }

        for (int j = 0; j < children; j++) {
            mindmaps.addRelation(arc)
                    .putRolePlayer(arcFrom, mindmaps.getInstance("a0"))
                    .putRolePlayer(arcTo, mindmaps.getInstance("a1," + j));
        }

        for(int i = 1 ; i < n ;i++) {
            int m = pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    mindmaps.addRelation(arc)
                            .putRolePlayer(arcFrom, mindmaps.getInstance("a" + i + "," + j))
                            .putRolePlayer(arcTo, mindmaps.getInstance("a" + (i + 1) + "," + (j * children + c)));

                }
                if (j!= 0 && j % 100 == 0)
                    System.out.println("level " + i + "/" + (n-1) + ": " + j + " entities out of " + m + " connected");
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathGraph loading time: " + loadTime + " ms");
    }
}
