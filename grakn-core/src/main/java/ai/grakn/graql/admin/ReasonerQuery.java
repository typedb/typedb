package ai.grakn.graql.admin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * conjunctive reasoner query interface
 */
public interface ReasonerQuery {

        /**
         * @return
         */
        GraknGraph graph();

        /**
         * @return
         */
        Conjunction<PatternAdmin> getPattern();

        Set<VarName> getSelectedNames();

        Set<Atomic> getAtoms();

        /**
         * @return true if any of the atoms constituting the query can be resolved through a rule
         */
        boolean isRuleResolvable();

        /**
         * change each variable occurrence in the query (apply unifier [from/to])
         * @param from variable name to be changed
         * @param to new variable name
         */
        void unify(VarName from, VarName to);

        /**
         * change each variable occurrence according to provided mappings (apply unifiers {[from, to]_i})
         * @param unifiers contain unifiers (variable mappings) to be applied
         */
        void unify(Map<VarName, VarName> unifiers);

        /**
         * resolves the query
         * @param materialise materialisation flag
         * @return stream of answers
         */
        Stream<Map<VarName, Concept>> resolve(boolean materialise);


        Map<VarName, Type> getVarTypeMap();
}
