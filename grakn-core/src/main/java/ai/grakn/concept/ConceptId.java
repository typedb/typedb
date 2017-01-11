package ai.grakn.concept;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *     A Concept Id
 * </p>
 *
 * <p>
 *     A class which represents an id of any {@link Concept} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing concept IDs from Strings.
 * </p>
 *
 * @author fppt
 */
public class ConceptId implements Comparable<ConceptId> {
    private static Map<String, ConceptId> conceptIds = new HashMap();

    private String conceptId;

    private ConceptId(String conceptId){
        this.conceptId = conceptId;
    }

    public String getValue(){
        return conceptId;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ConceptId && ((ConceptId) object).getValue().equals(conceptId);
    }

    @Override
    public int compareTo(ConceptId o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public String toString(){
        return conceptId;
    }

    /**
     *
     * @param value The string which potentially represents a Concept
     * @return The matching concept ID
     */
    public static ConceptId of(String value){
        return conceptIds.computeIfAbsent(value, ConceptId::new);
    }
}
