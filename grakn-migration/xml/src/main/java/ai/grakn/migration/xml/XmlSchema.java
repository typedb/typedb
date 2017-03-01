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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Read an XML Schema (a .xsd) file and provide information about the types inside it.
 * 
 * @author borislav
 *
 */
public class XmlSchema {
    private Map<String, String> types = new HashMap<String, String>();
    
    public String typeOf(String elementName) {
        return types.get(elementName);
    }
    
    public XmlSchema read(File schemaFile) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(schemaFile); 
            NodeList list = doc.getElementsByTagName("xs:element");
            for (int i = 0; i < list.getLength(); i++) {
                Element el = (Element)list.item(i);
                if (el.hasAttribute("type")) {
                    types.put(el.getAttribute("name"), el.getAttribute("type"));
                }
                else { 
                    types.put(el.getAttribute("name"), "xs:complexType");
                }
            }
            return this;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
