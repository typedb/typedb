package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.InvalidConceptValueException;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

class ResourceImpl<D> extends InstanceImpl<Resource<D>, ResourceType<D>, D> implements Resource<D> {
    ResourceImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public Data<D> dataType() {
        return type().getDataType();
    }

    @Override
    public Collection<Instance> ownerInstances() {
        Set<Instance> owners = new HashSet<>();
        this.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).forEach(concept -> {
            if(!concept.getBaseType().equals(DataType.BaseType.RESOURCE.name()))
                owners.add(getMindmapsGraph().getElementFactory().buildSpecificInstance(concept));
        });
        return owners;
    }

    @Override
    public Resource<D> setValue(D value) {
        try {
            ResourceType<D> resourceType = type();

            //Not checking the datatype because the regex will always be null for non strings.
            String regex = resourceType.getRegex();
            if(regex != null){
                if(!Pattern.matches(regex, (String) value)){
                    throw new InvalidConceptValueException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, toString()));
                }
            }

            //If the value has to be unique some additional checks are in order
            if(type().isUnique()){
                String index = generateResourceIndex(value);
                ConceptImpl<?, ?, ?> conceptByIndex = mindmapsGraph.getConcept(DataType.ConceptPropertyUnique.INDEX, index);
                if(conceptByIndex != null && !conceptByIndex.getId().equals(getId())){
                    throw new InvalidConceptValueException(ErrorMessage.RESOURCE_CANNOT_HAVE_VALUE.getMessage(value, this, conceptByIndex));
                } else {
                    setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, index);
                }
            }

            return setProperty(dataType().getConceptProperty(), castValue(value));
        } catch (ClassCastException e) {
            throw new RuntimeException(ErrorMessage.INVALID_DATATYPE.getMessage(value, this, dataType().getName()));
        }
    }

    public String generateResourceIndex(D value){
        return DataType.BaseType.RESOURCE.name() + "-" + type().getId() + "-" + value.toString();
    }

    private Object castValue(Object value){
        Data<D> parentDataType = dataType();
        if(parentDataType.equals(Data.DOUBLE)){
            return ((Number) value).doubleValue();
        } else if(parentDataType.equals(Data.LONG)){
            if(value instanceof Double){
                throw new ClassCastException();
            }
            return ((Number) value).longValue();
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public D getValue(){
        return (D) getProperty(dataType().getConceptProperty());
    }
}
