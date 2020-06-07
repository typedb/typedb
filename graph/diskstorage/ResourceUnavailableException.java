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

package grakn.core.graph.diskstorage;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphException;
import org.apache.commons.lang.StringUtils;

/**
 * This exception is thrown if a resource is being accessed that is unavailable.
 * The resource can be an external storage system, indexing system or other component.
 * <p>
 *
 */
public class ResourceUnavailableException extends JanusGraphException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public ResourceUnavailableException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public ResourceUnavailableException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public ResourceUnavailableException(Throwable cause) {
        this("Attempting to access unavailable resource", cause);
    }

    public static void verifyOpen(boolean isOpen, String resourceName, String... resourceIdentifiers) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resourceName));
        if (!isOpen) {
            StringBuilder msg = new StringBuilder();
            msg.append(resourceName).append(" ");
            if (resourceIdentifiers!=null && resourceIdentifiers.length>0) {
                msg.append("[");
                for (int i = 0; i < resourceIdentifiers.length; i++) {
                    if (i>0) msg.append(",");
                    msg.append(resourceIdentifiers[i]);
                }
                msg.append("] ");
            }
            msg.append("has been closed");
            throw new ResourceUnavailableException(msg.toString());
        }
    }


}
