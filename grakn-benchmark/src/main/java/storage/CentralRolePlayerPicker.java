package storage;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import pdf.PDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class CentralRolePlayerPicker implements RolePlayerConceptPickerInterface {

    private Boolean isReset;
    private Stream<String> idStream;
    private String typeLabel;
    private String relationshipLabel;
    private String roleLabel;
    private ConceptTypeCountStore conceptTypeCountStore;
    private Random rand;
    private ArrayList<String> idList;

    public CentralRolePlayerPicker(Random rand, String typeLabel, String relationshipLabel, String roleLabel, ConceptTypeCountStore conceptTypeCountStore) {
        this.typeLabel = typeLabel;
        this.relationshipLabel = relationshipLabel;
        this.roleLabel = roleLabel;
        this.conceptTypeCountStore = conceptTypeCountStore;
        this.rand = rand;
        this.isReset = true;
    }

    @Override
    public Stream<String> get(PDF pdf, GraknTx tx) {
        // Only create a new stream if reset() has been called prior
        if (this.isReset) {
            // Then create the stream
            int numInstances = pdf.next();
            this.idList = new ArrayList<>();

            // TODO Do this without replacement
            // TODO That is, keep track of the random offsets created and reject if duplicate
            for (int i = 0; i<= numInstances; i++){
                boolean resultCheck = true;
                QueryBuilder qb = tx.graql();
                Concept cRes = null;

                while (resultCheck) {
                    int randomOffset = RandomConceptPicker.generateRandomOffset(this.conceptTypeCountStore, this.typeLabel, this.rand);
                    cRes = this.conceptFromOffset(qb, randomOffset);
                    resultCheck = this.checkRelationshipExists(qb, cRes);
                }

                this.idList.add(cRes.getId().toString());
            }

            this.isReset = false;
        }
        // Return the same stream as before
        return this.idList.stream();
    }

    private Concept conceptFromOffset(QueryBuilder qb, Integer offset) {
        Var c = Graql.var().asUserDefined();
//        List<Answer> result = qb.match(c.isa(this.typeLabel))
//                .offset(offset)
//                .limit(1)
//                .get()
//                .execute();

        GetQuery query = qb.match(c.isa(this.typeLabel))
                .offset(offset)
                .limit(1)
                .get();
        List<Answer> result = query.execute();
        Answer res = result.get(0);
        return res.get(c);
    }

    private Boolean checkRelationshipExists(QueryBuilder qb, Concept concept) {
        Var r = Graql.var().asUserDefined();
        Var c = Graql.var().asUserDefined();

        long count = qb.match(c.id(concept.getId()), r.rel(this.roleLabel, c).isa(this.relationshipLabel)).get().execute().size();

        // TODO Using the following threw this error
        // ai.grakn.remote.RemoteGraknTx cannot be cast to ai.grakn.kb.internal.EmbeddedGraknTx
//        Stream<Concept> query = qb.match(c.id(concept.getId()), r.rel(this.roleLabel, c).isa(this.relationshipLabel)).get(r);
//        long count = query.count();

        return count != 0;
    }

    @Override
    public void reset() {
        this.isReset = true;
    }
}

