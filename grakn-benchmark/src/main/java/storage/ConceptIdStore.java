package storage;

import ai.grakn.concept.Concept;

import java.util.ArrayList;
import java.util.HashMap;

public class ConceptIdStore implements ConceptStore {

    private final HashMap<String, ArrayList<String>> conceptIdStorage;

    public ConceptIdStore() {
        this.conceptIdStorage = new HashMap<String, ArrayList<String>>();
    }

    public void add(Concept concept) {

//        String conceptClassLabel = concept.getClass().toString();
        String conceptClassLabel = concept.asThing().type().getLabel().toString();
//        String conceptClassLabel = ((AutoValue_RemoteEntity) concept).type().getLabel().toString();
        String conceptId = concept.getId().toString();

        this.conceptIdStorage.computeIfAbsent(conceptClassLabel, k -> new ArrayList<String>()).add(conceptId);
    }

    public ArrayList<String> get(String type) {
        return this.conceptIdStorage.get(type);
    }
}
