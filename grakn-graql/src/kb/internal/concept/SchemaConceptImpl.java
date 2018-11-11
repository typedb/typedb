/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.kb.internal.concept;

import grakn.core.concept.Concept;
import grakn.core.concept.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.Role;
import grakn.core.concept.Rule;
import grakn.core.concept.SchemaConcept;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.exception.PropertyNotUniqueException;
import grakn.core.kb.internal.cache.Cache;
import grakn.core.kb.internal.cache.Cacheable;
import grakn.core.kb.internal.structure.VertexElement;
import grakn.core.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static scala.tools.scalap.scalax.rules.scalasig.NoSymbol.isAbstract;

/**
 * <p>
 *     Schema Specific {@link Concept}
 * </p>
 *
 * <p>
 *     Allows you to create schema or ontological elements.
 *     These differ from normal graph constructs in two ways:
 *     1. They have a unique {@link Label} which identifies them
 *     2. You can link them together into a hierarchical structure
 * </p>
 *
 *
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType} or {@link RelationshipType} or {@link Role}
 */
public abstract class SchemaConceptImpl<T extends SchemaConcept> extends ConceptImpl implements SchemaConcept {
    private final Cache<Label> cachedLabel = Cache.createPersistentCache(this, Cacheable.label(), () ->  Label.of(vertex().property(Schema.VertexProperty.SCHEMA_LABEL)));
    private final Cache<LabelId> cachedLabelId = Cache.createSessionCache(this, Cacheable.labelId(), () -> LabelId.of(vertex().property(Schema.VertexProperty.LABEL_ID)));
    private final Cache<T> cachedSuperType = Cache.createSessionCache(this, Cacheable.concept(), () -> this.<T>neighbours(Direction.OUT, Schema.EdgeLabel.SUB).findFirst().orElse(null));
    private final Cache<Set<T>> cachedDirectSubTypes = Cache.createSessionCache(this, Cacheable.set(), () -> this.<T>neighbours(Direction.IN, Schema.EdgeLabel.SUB).collect(Collectors.toSet()));
    private final Cache<Boolean> cachedIsImplicit = Cache.createSessionCache(this, Cacheable.bool(), () -> vertex().propertyBoolean(Schema.VertexProperty.IS_IMPLICIT));

    SchemaConceptImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    SchemaConceptImpl(VertexElement vertexElement, T superType) {
        this(vertexElement);
        if(sup() == null) sup(superType);
    }

    public T label(Label label){
        try {
            vertex().tx().txCache().remove(this);
            vertex().propertyUnique(Schema.VertexProperty.SCHEMA_LABEL, label.getValue());
            cachedLabel.set(label);
            vertex().tx().txCache().cacheConcept(this);
            return getThis();
        } catch (PropertyNotUniqueException exception){
            vertex().tx().txCache().cacheConcept(this);
            throw GraknTxOperationException.labelTaken(label);
        }
    }

    /**
     *
     * @return The internal id which is used for fast lookups
     */
    @Override
    public LabelId labelId(){
        return cachedLabelId.get();
    }

    /**
     *
     * @return The label of this ontological element
     */
    @Override
    public Label label() {
        return cachedLabel.get();
    }

    /**
     *
     * @return The super of this {@link SchemaConcept}
     */
    public T sup() {
        return cachedSuperType.get();
    }


    @Override
    public Stream<T> sups() {
        Set<T> superSet= new HashSet<>();

        T superParent = getThis();

        while(superParent != null && !Schema.MetaSchema.THING.getLabel().equals(superParent.label())){
            superSet.add(superParent);

            //noinspection unchecked
            superParent = (T) superParent.sup();
        }

        return superSet.stream();
    }

    /**
     *
     * @return returns true if the type was created implicitly through the resource syntax
     */
    @Override
    public Boolean isImplicit(){
        return cachedIsImplicit.get();
    }

    /**
     * Deletes the concept as a {@link SchemaConcept}
     */
    @Override
    public void delete(){
        if(deletionAllowed()){
            //Force load of linked concepts whose caches need to be updated
            T superConcept = cachedSuperType.get();

            deleteNode();

            //Update neighbouring caches
            //noinspection unchecked
            SchemaConceptImpl.from(superConcept).deleteCachedDirectedSubType(getThis());

            //Clear Global Cache
            vertex().tx().txCache().remove(this);

            //Clear internal caching
            txCacheClear();
        } else {
            throw GraknTxOperationException.cannotBeDeleted(this);
        }
    }

    boolean deletionAllowed(){
        checkSchemaMutationAllowed();
        return !neighbours(Direction.IN, Schema.EdgeLabel.SUB).findAny().isPresent();
    }

    /**
     *
     * @return All the subs of this concept including itself
     */
    @Override
    public Stream<T> subs(){
        return nextSubLevel(getThis());
    }

    /**
     * Adds a new sub type to the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param newSubType The new subtype
     */
    private void addCachedDirectSubType(T newSubType){
        cachedDirectSubTypes.ifPresent(set -> set.add(newSubType));
    }

    /**
     *
     * @param root The current {@link SchemaConcept}
     * @return All the sub children of the root. Effectively calls  the cache {@link SchemaConceptImpl#cachedDirectSubTypes} recursively
     */
    @SuppressWarnings("unchecked")
    private Stream<T> nextSubLevel(T root){
        return Stream.concat(Stream.of(root),
                SchemaConceptImpl.<T>from(root).cachedDirectSubTypes.get().stream().flatMap(this::nextSubLevel));
    }

    /**
     * Checks if we are mutating a {@link SchemaConcept} in a valid way. {@link SchemaConcept} mutations are valid if:
     * 1. The {@link SchemaConcept} is not a meta-type
     * 2. The graph is not batch loading
     */
    void checkSchemaMutationAllowed(){
        vertex().tx().checkSchemaMutationAllowed();
        if(Schema.MetaSchema.isMetaLabel(label())){
            throw GraknTxOperationException.metaTypeImmutable(label());
        }
    }

    /**
     * Removes an old sub type from the currently cached sub types. If no subtypes have been cached then this will hit the database.
     *
     * @param oldSubType The old sub type which should not be cached anymore
     */
    private void deleteCachedDirectedSubType(T oldSubType){
        cachedDirectSubTypes.ifPresent(set -> set.remove(oldSubType));
    }

    /**
     *
     * @param newSuperType This type's super type
     * @return The Type itself
     */
    public T sup(T newSuperType) {
        T oldSuperType = sup();
        if(changingSuperAllowed(oldSuperType, newSuperType)){
            //Update the super type of this type in cache
            cachedSuperType.set(newSuperType);

            //Note the check before the actual construction
            if(superLoops()){
                cachedSuperType.set(oldSuperType); //Reset if the new super type causes a loop
                throw GraknTxOperationException.loopCreated(this, newSuperType);
            }

            //Modify the graph once we have checked no loop occurs
            deleteEdge(Direction.OUT, Schema.EdgeLabel.SUB);
            putEdge(ConceptVertex.from(newSuperType), Schema.EdgeLabel.SUB);

            //Update the sub types of the old super type
            if(oldSuperType != null) {
                //noinspection unchecked - Casting is needed to access {deleteCachedDirectedSubTypes} method
                ((SchemaConceptImpl<T>) oldSuperType).deleteCachedDirectedSubType(getThis());
            }

            //Add this as the subtype to the supertype
            //noinspection unchecked - Casting is needed to access {addCachedDirectSubTypes} method
            ((SchemaConceptImpl<T>) newSuperType).addCachedDirectSubType(getThis());

            //Track any existing data if there is some
            if(oldSuperType != null) trackRolePlayers();
        }
        return getThis();
    }


    /**
     * Checks if changing the super is allowed. This passed if:
     * 1. The transaction is not of type {@link grakn.core.GraknTxType#BATCH}
     * 2. The <code>newSuperType</code> is different from the old.
     *
     * @param oldSuperType the old super
     * @param newSuperType the new super
     * @return true if we can set the new super
     */
    boolean changingSuperAllowed(T oldSuperType, T newSuperType){
        checkSchemaMutationAllowed();
        return oldSuperType == null || !oldSuperType.equals(newSuperType);
    }

    /**
     * Method which performs tasks needed in order to track super changes properly
     */
    abstract void trackRolePlayers();

    private boolean superLoops(){
        //Check For Loop
        HashSet<SchemaConcept> foundTypes = new HashSet<>();
        SchemaConcept currentSuperType = sup();
        while (currentSuperType != null){
            foundTypes.add(currentSuperType);
            currentSuperType = currentSuperType.sup();
            if(foundTypes.contains(currentSuperType)){
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @return A collection of {@link Rule} for which this {@link SchemaConcept} serves as a hypothesis
     */
    @Override
    public Stream<Rule> whenRules() {
        return neighbours(Direction.IN, Schema.EdgeLabel.HYPOTHESIS);
    }

    /**
     *
     * @return A collection of {@link Rule} for which this {@link SchemaConcept} serves as a conclusion
     */
    @Override
    public Stream<Rule> thenRules() {
        return neighbours(Direction.IN, Schema.EdgeLabel.CONCLUSION);
    }

    @Override
    public String innerToString(){
        String message = super.innerToString();
        message = message + " - Label [" + label() + "] - Abstract [" + isAbstract() + "] ";
        return message;
    }

    public static <X extends SchemaConcept> SchemaConceptImpl<X> from(SchemaConcept schemaConcept){
        //noinspection unchecked
        return (SchemaConceptImpl<X>) schemaConcept;
    }
}
