/*
<<<<<<< HEAD
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
=======
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
>>>>>>> 88134929f3f653aa13bfa69ca7d0727344cc1e9a
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
<<<<<<< HEAD
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
=======
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
>>>>>>> 88134929f3f653aa13bfa69ca7d0727344cc1e9a
 */

package ai.grakn.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;

/**
 * Repeat annotation to be applied at the test method level to indicate how many times the test method shall be executed.
 * Used in conjunction with {@link RepeatRule}.
 *
 */
@Retention( RetentionPolicy.RUNTIME )
@Target(METHOD)
public @interface Repeat {
    int times();
}
