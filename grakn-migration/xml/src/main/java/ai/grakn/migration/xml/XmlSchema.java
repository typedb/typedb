/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.migration.xml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Read an XML Schema (a .xsd) file and provide information about the types inside it.
 * 
 * @author borislav
 *
 */
public class XmlSchema {

    /**
     * Holds type information about an XML element: the XML Schema type name and max number of occurrences.
     */
    public static class TypeInfo {
        private String name;
        private long cardinality;
        public TypeInfo(String name, long cardinality) {
            this.name = name;
            this.cardinality = cardinality;
        }        
        public String name() { return name; }
        public long cardinality() { return cardinality; }
    }
    
    private Map<String, TypeInfo> types = new HashMap<String, TypeInfo>();
    
    /**
     * Return the type info of an element or <code>new TypeInfo("xs:complexType", Long.MAX_LONG)</code> as
     * a default.
     */
    public TypeInfo typeOf(String elementName) {
        TypeInfo ti = types.get(elementName);
        return ti == null ? new TypeInfo("xs:complexType", 1) : ti;
    }
    
    public XmlSchema read(File schemaFile) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(schemaFile); 
            NodeList list = doc.getElementsByTagName("xs:element");
            for (int i = 0; i < list.getLength(); i++) {
                Element el = (Element)list.item(i);
                long cardinality = 1;
                if (el.hasAttribute("maxOccurs")) {
                    cardinality = ("unbounded".equals(el.getAttribute("maxOccurs"))) ? 
                                  Long.MAX_VALUE :
                                  Long.parseLong(el.getAttribute("maxOccurs"));
                }
                if (el.hasAttribute("type")) {
                    types.put(el.getAttribute("name"), new TypeInfo(el.getAttribute("type"), cardinality));
                }
                else { 
                    types.put(el.getAttribute("name"), new TypeInfo("xs:complexType", cardinality));
                }
            }
            return this;
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
