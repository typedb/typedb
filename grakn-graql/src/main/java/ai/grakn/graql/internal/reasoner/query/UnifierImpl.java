package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kasper on 23/03/17.
 */
public class UnifierImpl implements Unifier {

    private Map<VarName, Set<VarName>> unifier = new HashMap<>();

    public Set<VarName> put(VarName key, VarName value){
        if (unifier.containsKey(key)){
            Set<VarName> val = unifier.get(key);
            val.add(value);
            return val;
        }
        else return unifier.put(key, Sets.newHashSet(value));
    }
}
