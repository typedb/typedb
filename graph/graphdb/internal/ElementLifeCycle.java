/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.internal;

import com.google.common.base.Preconditions;

/**
 * ElementLifeCycle enumerates all possible states of the lifecycle of a entity.
 *
;
 */
public class ElementLifeCycle {

    public enum Event {REMOVED, REMOVED_RELATION, ADDED_RELATION, UPDATE }

    /**
     * The entity has been newly created and not yet persisted.
     */
    public final static byte New = 1;

    /**
     * The entity has been loaded from the database and has not changed
     * after initial loading.
     */
    public final static byte Loaded = 2;

    /**
     * The entity has changed after being loaded from the database by adding relations.
     */
    private final static byte AddedRelations = 3;

    /**
     * The entity has changed after being loaded from the database by deleting relations.
     */
    private final static byte RemovedRelations = 4;

    /**
     * The entity has changed after being loaded from the database by adding and/or deleting relations.
     */
    private final static byte Modified = 5;

    /**
     * The entity has been deleted but not yet erased from the database.
     */
    public final static byte Removed = 6;


    public static boolean isModified(byte lifecycle) {
        return lifecycle>=AddedRelations && lifecycle<=Modified;
    }

    public static boolean hasRemovedRelations(byte lifecycle) {
        return lifecycle== RemovedRelations || lifecycle==Modified;
    }

    public static boolean hasAddedRelations(byte lifecycle) {
        return lifecycle==AddedRelations || lifecycle==Modified;
    }


    public static boolean isNew(byte lifecycle) {
        return lifecycle==New;
    }

    public static boolean isLoaded(byte lifecycle) {
        return lifecycle==Loaded;
    }

    public static boolean isRemoved(byte lifecycle) {
        return lifecycle== Removed;
    }

    public static boolean isValid(byte lifecycle) {
        return lifecycle>=New && lifecycle<=Removed;
    }

    public static byte update(byte lifecycle, Event event) {
        Preconditions.checkArgument(isValid(lifecycle),"Invalid element state: " + lifecycle);
        if (event== Event.REMOVED) return Removed;
        else if (lifecycle==New || lifecycle==Modified) {
            return lifecycle;
        } else if (lifecycle== Removed) {
            throw new IllegalStateException("No event can occur on deleted vertices: " + event);
        } else if (event== Event.REMOVED_RELATION) {
            switch (lifecycle) {
                case Loaded:
                    return RemovedRelations;
                case AddedRelations:
                    return Modified;
                case RemovedRelations:
                    return RemovedRelations;
                default:
                    throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
            }
        } else if (event== Event.ADDED_RELATION) {
            switch (lifecycle) {
                case Loaded:
                    return AddedRelations;
                case RemovedRelations:
                    return Modified;
                case AddedRelations:
                    return AddedRelations;
                default:
                    throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
            }
        } else if (event== Event.UPDATE) {
            return Modified;
        } else throw new IllegalStateException("Unexpected state event: " + lifecycle + " - " + event);
    }


}
