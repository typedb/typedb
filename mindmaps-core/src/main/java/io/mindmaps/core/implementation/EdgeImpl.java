package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.GraphRuntimeException;
import org.apache.tinkerpop.gremlin.structure.Edge;

class EdgeImpl {
    private Edge edge;
    private final MindmapsTransactionImpl mindmapsGraph;

    EdgeImpl(org.apache.tinkerpop.gremlin.structure.Edge e, MindmapsTransactionImpl mindmapsGraph){
        edge = e;
        this.mindmapsGraph = mindmapsGraph;
    }

    public void delete(){
        mindmapsGraph.getTransaction().putConcept(getToConcept());
        mindmapsGraph.getTransaction().putConcept(getFromConcept());

        edge.remove();
        edge = null;
    }

    public Object getId(){
        return edge.id();
    }

    @Override
    public int hashCode() {
        return edge.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof EdgeImpl && ((EdgeImpl) object).edgeEquals(edge);
    }

    public ConceptImpl getFromConcept(){
        return mindmapsGraph.getElementFactory().buildUnknownConcept(edge.outVertex());
    }

    public ConceptImpl getToConcept(){
        return mindmapsGraph.getElementFactory().buildUnknownConcept(edge.inVertex());
    }

    public DataType.EdgeLabel getType() {
        DataType.EdgeLabel label = DataType.EdgeLabel.getEdgeLabel(edge.label());
        if(label == null)
            throw new GraphRuntimeException(ErrorMessage.INVALID_EDGE.getMessage(edge.label(), getFromConcept().getId(), getToConcept().getId()));
        return label;
    }

    public String getEdgePropertyRoleType() {
        return (String) getEdgeProperty(DataType.EdgeProperty.ROLE_TYPE);
    }

    public void setEdgePropertyRoleType(String value) {
        setEdgeProperty(DataType.EdgeProperty.ROLE_TYPE, value);
    }

    public String getEdgePropertyRelationId() {
        return (String) getEdgeProperty(DataType.EdgeProperty.RELATION_ID);
    }

    public void setEdgePropertyRelationId(String value) {
        setEdgeProperty(DataType.EdgeProperty.RELATION_ID, value);
    }

    public String getEdgePropertyToId() {
        return (String) getEdgeProperty(DataType.EdgeProperty.TO_ID);
    }

    public void setEdgePropertyToId(String value) {
        setEdgeProperty(DataType.EdgeProperty.TO_ID, value);
    }

    public String getEdgePropertyToRole() {
        return (String) getEdgeProperty(DataType.EdgeProperty.TO_ROLE);
    }

    public void setEdgePropertyToRole(String value) {
        setEdgeProperty(DataType.EdgeProperty.TO_ROLE, value);
    }

    public String getEdgePropertyToType() {
        return (String) getEdgeProperty(DataType.EdgeProperty.TO_TYPE);
    }

    public void setEdgePropertyToType(String value) {
        setEdgeProperty(DataType.EdgeProperty.TO_TYPE, value);
    }

    public String getEdgePropertyFromId() {
        return (String) getEdgeProperty(DataType.EdgeProperty.FROM_ID);
    }

    public void setEdgePropertyFromId(String value) {
        setEdgeProperty(DataType.EdgeProperty.FROM_ID, value);
    }

    public String getEdgePropertyFromRole() {
        return (String) getEdgeProperty(DataType.EdgeProperty.FROM_ROLE);
    }

    public void setEdgePropertyFromRole(String value) {
        setEdgeProperty(DataType.EdgeProperty.FROM_ROLE, value);
    }

    public String getEdgePropertyFromType() {
        return (String) getEdgeProperty(DataType.EdgeProperty.FROM_TYPE);
    }

    public void setEdgePropertyFromType(String value) {
        setEdgeProperty(DataType.EdgeProperty.FROM_TYPE, value);
    }

    public Long getEdgePropertyBaseAssertionId() {
        return (Long) getEdgeProperty(DataType.EdgeProperty.ASSERTION_BASE_ID);
    }

    public void setEdgePropertyBaseAssertionId(Long value) {
        setEdgeProperty(DataType.EdgeProperty.ASSERTION_BASE_ID, value);
    }

    public String getEdgePropertyShortcutHash() {
        return (String) getEdgeProperty(DataType.EdgeProperty.SHORTCUT_HASH);
    }

    public void setEdgePropertyShortcutHash(String hash){
        setEdgeProperty(DataType.EdgeProperty.SHORTCUT_HASH, hash);
    }

    public String getEdgePropertyValue() {
        return (String) getEdgeProperty(DataType.EdgeProperty.VALUE);
    }

    public void setEdgePropertyValue(String value) {
        setEdgeProperty(DataType.EdgeProperty.VALUE, value);
    }

    private Object getEdgeProperty(DataType.EdgeProperty type){
        org.apache.tinkerpop.gremlin.structure.Property property = edge.property(type.name());
        if(property != null && property.isPresent())
            return property.value();
        else
            return null;
    }

    private void setEdgeProperty(DataType.EdgeProperty type, Object value){
        edge.property(type.name(), value);
    }

    private boolean edgeEquals(org.apache.tinkerpop.gremlin.structure.Edge toEdge) {
        return edge.equals(toEdge);
    }
}
