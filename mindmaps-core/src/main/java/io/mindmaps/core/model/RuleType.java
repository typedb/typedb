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
