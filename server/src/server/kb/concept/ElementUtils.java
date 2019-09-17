package grakn.core.server.kb.concept;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.JanusGraphElement;

public class ElementUtils {
    public static boolean isValidElement(Element element) {
        return element != null && !((JanusGraphElement) element).isRemoved();
    }
}
