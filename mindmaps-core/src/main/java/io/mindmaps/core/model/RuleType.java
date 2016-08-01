/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.model;

import java.util.Collection;

public interface RuleType extends Type {
    //---- Inherited Methods
    RuleType setId(String id);
    RuleType setSubject(String subject);
    RuleType setValue(String value);
    RuleType setAbstract(Boolean isAbstract);
    RuleType superType();
    RuleType superType(RuleType type);
    Collection<RuleType> subTypes();
    RuleType playsRole(RoleType roleType);
    RuleType deletePlaysRole(RoleType roleType);
    Collection<Rule> instances();
}
