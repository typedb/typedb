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

package grakn.core.server.migrate;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import graql.lang.pattern.Pattern;

import java.io.Writer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Schema {

    private final Session session;
    private Transaction tx;

    public Schema(Session session) {
        this.session = session;
    }

    private final HashMap<String, List<TypeDeclaration>> superMap = new HashMap<>();
    private final HashMap<String, TypeDeclaration> labelMap = new HashMap<>();

    private List<String> attributeOrder = new ArrayList<>();
    private List<String> relationOrder = new ArrayList<>();

    public void printSchema(Writer out) {
        tx = session.transaction(Transaction.Type.READ);

        IndentPrintWriter writer = new IndentPrintWriter(out, "    ");

        writer.println("define");
        writer.println();

        AttributeType<?> metaAttribute = (AttributeType<?>) tx.getMetaAttributeType();
        metaAttribute.subs()
                .filter(at -> !metaAttribute.equals(at) && !at.isImplicit())
                .forEach(TypeDeclaration::new);

        EntityType metaEntity = tx.getMetaEntityType();
        metaEntity.subs()
                .filter(et -> !metaEntity.equals(et) && !et.isImplicit())
                .forEach(TypeDeclaration::new);

        RelationType metaRelationType = tx.getMetaRelationType();
        metaRelationType.subs()
                .filter(rt -> !metaRelationType.equals(rt) && !rt.isImplicit())
                .forEach(TypeDeclaration::new);

        Rule metaRule = tx.getMetaRule();
        metaRule.subs()
                .filter(r -> !metaRule.equals(r))
                .forEach(TypeDeclaration::new);

        orderLabelsHierarchically(attributeOrder, "attribute");
        orderLabelsHierarchically(relationOrder, "relation");

        printTypesHierarchically(writer, "attribute");
        printTypesHierarchically(writer, "entity");
        printTypesHierarchically(writer, "relation");

        tx.close();
    }

    private void orderLabelsHierarchically(List<String> ordering, String startingLabel) {
        ordering.add(startingLabel);
        List<TypeDeclaration> children = superMap.get(startingLabel);
        if (children == null) return;

        children.sort(Comparator.comparing(td -> td.label));
        for (TypeDeclaration child : children) {
            orderLabelsHierarchically(ordering, child.label);
        }
    }

    private void printTypesHierarchically(IndentPrintWriter writer, String startingLabel) {
        List<TypeDeclaration> children = superMap.get(startingLabel);
        if (children == null) return;

        children.sort(Comparator.comparing(td -> td.label));
        for (TypeDeclaration child : children) {
            child.print(writer);
            writer.println();
            writer.indent();
            printTypesHierarchically(writer, child.label);
            writer.unindent();
        }
    }

    private class RelatesDeclaration {
        private final String label;
        private final String as;

        RelatesDeclaration(Role role) {
            label = role.label().toString();
            String supLabel = Objects.requireNonNull(role.sup()).label().toString();
            if (!"role".equals(supLabel)) {
                as = supLabel;
            } else {
                as = null;
            }
        }

        public void print(IndentPrintWriter writer) {
            writer.print(label);
            if (as != null) {
                writer.print(" as ");
                writer.print(as);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RelatesDeclaration)) return false;
            RelatesDeclaration that = (RelatesDeclaration) o;
            return Objects.equals(label, that.label) &&
                    Objects.equals(as, that.as);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(label, as);
        }
    }

    List<String> order(Set<String> toOrder, List<String> master) {
        List<String> results = new ArrayList<>(toOrder.size());
        for (String s : master) {
            if (toOrder.contains(s)) {
                results.add(s);
            }
        }
        return results;
    }

    private class TypeDeclaration {
        private final String label;
        private final String superType;
        private final String dataType;
        private final boolean isAbstract;
        private final Set<String> key;
        private final Set<String> has;
        private final Set<String> plays;
        private final Set<RelatesDeclaration> relates;
        private final Pattern when;
        private final Pattern then;

        TypeDeclaration(SchemaConcept sc) {
            label = sc.label().toString();
            labelMap.put(label, this);

            superType = Objects.requireNonNull(sc.sup()).label().toString();
            superMap.computeIfAbsent(superType, st -> new ArrayList<>()).add(this);

            if (sc.isAttributeType()) {
                AttributeType.DataType<?> dataType = sc.asAttributeType().dataType();
                if (dataType != null) {
                    this.dataType = convertDataType(dataType.dataClass());
                } else {
                    this.dataType = null;
                }
            } else {
                dataType = null;
            }

            if (sc.isType()) {
                Type type = sc.asType();
                isAbstract = type.isAbstract();
                key = type.keys()
                        .map(t -> t.label().toString())
                        .collect(Collectors.toSet());

                has = type.attributes()
                        .map(t -> t.label().toString())
                        .filter(l -> !key.contains(l))
                        .collect(Collectors.toSet());

                plays = type.playing()
                        .filter(r -> !r.isImplicit())
                        .map(t -> t.label().toString())
                        .collect(Collectors.toSet());

                TypeDeclaration parent = labelMap.get(superType);
                if (parent != null) {
                    for (String parentKey : parent.key) {
                        key.remove(parentKey);
                    }
                    for (String parentHas : parent.has) {
                        has.remove(parentHas);
                    }
                    for (String parentPlays : parent.plays) {
                        plays.remove(parentPlays);
                    }
                }
            } else {
                isAbstract = false;
                key = Collections.emptySet();
                has = Collections.emptySet();
                plays = Collections.emptySet();
            }

            if (sc.isRelationType()) {
                RelationType relationType = sc.asRelationType();
                relates = relationType.roles()
                        .filter(r -> !r.isImplicit())
                        .map(RelatesDeclaration::new)
                        .collect(Collectors.toSet());

                TypeDeclaration parent = labelMap.get(superType);
                if (parent != null) {
                    for (RelatesDeclaration parentRelates : parent.relates) {
                        relates.remove(parentRelates);
                    }
                }
            } else {
                relates = Collections.emptySet();
            }

            if (sc.isRule()) {
                Rule rule = sc.asRule();
                when = rule.when();
                then = rule.then();
            } else {
                when = null;
                then = null;
            }
        }

        public void print(IndentPrintWriter writer) {
            writer.print(label);
            writer.print(" sub ");
            writer.print(superType);
            writer.indent();
            if (isAbstract) {
                writer.println(",");
                writer.print("abstract");
            }
            if (dataType != null) {
                writer.println(",");
                writer.print("datatype ");
                writer.print(dataType);
            }
            for (String key : order(this.key, attributeOrder)) {
                writer.println(",");
                writer.print("key ");
                writer.print(key);
            }
            for (String has : order(this.has, attributeOrder)) {
                writer.println(",");
                writer.print("has ");
                writer.print(has);
            }
            for (String plays : this.plays.stream().sorted().collect(Collectors.toList())) {
                writer.println(",");
                writer.print("plays ");
                writer.print(plays);
            }
            for (RelatesDeclaration relates : this.relates.stream()
                    .sorted(Comparator.comparing(r -> r.label))
                    .collect(Collectors.toList())) {
                writer.println(",");
                writer.print("relates ");
                relates.print(writer);
            }
            if (when != null) {
                writer.println(",");
                writer.print("when ");
                String ruleString = when.toString();
                writer.print(ruleString.substring(0, ruleString.length() - 1));
            }
            if (then != null) {
                writer.println(",");
                writer.println("then ");
                String ruleString = then.toString();
                writer.print(ruleString.substring(0, ruleString.length() - 1));
            }
            writer.unindent();
            writer.println(";");
        }
    }

    public class IndentPrintWriter extends java.io.PrintWriter
    {
        private boolean newLine;
        private String singleIndent;
        private String currentIndent = "";

        public IndentPrintWriter(Writer pOut, String indent)
        {
            super(pOut);
            this.singleIndent = indent;
        }

        public void indent()
        {
            currentIndent += singleIndent;
        }

        public void unindent()
        {
            if (currentIndent.isEmpty()) return;
            currentIndent = currentIndent.substring(0, currentIndent.length() -   singleIndent.length());
        }

        @Override
        public void print(String pString)
        {
            synchronized (lock) {
                // indent when printing at the start of a new line
                if (newLine) {
                    super.print(currentIndent);
                    newLine = false;
                }

                // strip the last new line symbol (if there is one)
                boolean endsWithNewLine = pString.endsWith("\n");
                if (endsWithNewLine) pString = pString.substring(0, pString.length() - 1);

                // print the text (add indent after new-lines)
                pString = pString.replaceAll("\n", "\n" + currentIndent);
                super.print(pString);

                // finally add the stripped new-line symbol.
                if (endsWithNewLine) println();
            }
        }

        @Override
        public void println()
        {
            synchronized (lock) {
                super.println();
                newLine = true;
            }
        }
    }

    private static String convertDataType(Class<?> dataType) {
        if (dataType.equals(String.class)) {
            return "string";
        } else if (dataType.equals(Boolean.class)) {
            return "boolean";
        } else if (dataType.equals(Integer.class)) {
            return "long";
        } else if (dataType.equals(Long.class)) {
            return "long";
        } else if (dataType.equals(Float.class)) {
            return "double";
        } else if (dataType.equals(Double.class)) {
            return "double";
        } else if (dataType.equals(LocalDateTime.class)) {
            return "date";
        } else {
            throw new UnsupportedOperationException("Datatype not recognized.");
        }
    }
}
