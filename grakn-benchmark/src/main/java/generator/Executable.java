//package generator;
//
//import ai.grakn.GraknTx;
//import ai.grakn.concept.Concept;
//import ai.grakn.concept.ConceptId;
//import ai.grakn.graql.*;
//import ai.grakn.graql.admin.Answer;
//import ai.grakn.util.GraqlSyntax;
//
//import javax.annotation.Nullable;
//import java.util.List;
//import java.util.stream.Stream;
//
//public class Executable implements Query<List<Answer>>, Streamable<Answer> {
//
//    private GraknTx tx;
//
//    public Executable(GraknTx tx) {
//        this.tx = tx;
//    }
//
//    @Override
//    public Query<List<Answer>> withTx(GraknTx tx) {
//        return null;
//    }
//
//    @Override
//    public List<Answer> execute() {
//
//        long resultCheck = 1;
//        QueryBuilder qb = this.tx.graql();
//        Var r = Graql.var();
//        Var c = Graql.var();
//
//        while (resultCheck != 0) {
//            List<Answer> result = qb.match(c.isa("company"))
//                    .offset(randomBoundedNumber)
//                    .limit(1)
//                    .get()
//                    .execute();
//            Answer res = result.get(0);
//            Concept cRes = res.get(c);
//
//            resultCheck = qb.match(c.id(cRes.getId()), r.rel("employer", c).isa("employment")).get(r).count();
//        }
//
////        long resultCheck = qb.match(c.id(cRes.getId()), r.rel("employer", c).isa("employment")).get(r);
////        long resultCheck = qb.match(c.id(cRes.getId()), r.rel("employer", c).isa("employment")).get().execute();
//
////        Var x = Graql.var();
////        Var y = Graql.var();
////        Stream<Concept> r1 = qb.match(x).get(x);
////        List<Answer> r2 = qb.match(x).get(x, y).execute();
////        List<Answer> r3 = qb.match(x).get().execute();
////        long r4 = qb.match(x).get(x).count();
////
////        qb.compute(GraqlSyntax.Compute.Method.COUNT).of("person");
//
//
//
//        return null;
//    }
//
//    @Override
//    public boolean isReadOnly() {
//        return false;
//    }
//
//    @Nullable
//    @Override
//    public GraknTx tx() {
//        return null;
//    }
//
//    @Override
//    public Boolean inferring() {
//        return null;
//    }
//
//    @Override
//    public Stream<Answer> stream() {
//        return null;
//    }
//}
