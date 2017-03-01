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

import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.base.AbstractMigrator;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import static java.util.stream.Collectors.toSet;

/**
 * Migrator for migrating XML data into Grakn instances
 * @author alexandraorth
 */
public class XmlMigrator extends AbstractMigrator {
    private XmlSchema schema;
    private final Set<Reader> readers;
    private final String template;
    private String element;
    
    /**
     * Construct a XmlMigrator to migrate data in the given file or dir
     * @param template parametrized graql insert query
     * @param xmlFileOrDir either a XML file or a directory containing XML files
     */
    public XmlMigrator(String template, File xmlFileOrDir){
        File[] files = {xmlFileOrDir};
        if(xmlFileOrDir.isDirectory()){
            files = xmlFileOrDir.listFiles(xmlFiles);
        }

        this.readers = Stream.of(files).map(this::asReader).collect(toSet());
        this.template = template;
    }

    /**
     * Construct a XmlMigrator to migrate data in given reader
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     */
    public XmlMigrator(String template, Reader reader){
        this.readers = Sets.newHashSet(reader);
        this.template = template;
    }

    public XmlMigrator element(String element) {
        this.element = element;
        return this;
    }
    
    public XmlMigrator schema(XmlSchema schema) {
        this.schema = schema;
        return this;
    }
    
    /**
     * Migrate each of the given XML objects as an insert query
     * @return stream of parsed insert queries
     */
    @Override
    public Stream<InsertQuery> migrate(){
        return readers.stream()
                .flatMap(this::toXmlNodes)
                .map(this::digest)
                .map(data -> (Map<String, Object>)data)
                .map(data -> { System.out.println("Processing : " + data); return data; } )
                .flatMap(data -> template(template, data).stream());
    }

    /**
     * Close the readers
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        readers.forEach((reader) -> {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Convert data in XML element to a Map<String, Object> or plain text depending on its content.
     * 
     * @param data XML element (a tag) to convert
     * @return A String containing the text content of the element or a Map with its nested elements.
     */
    Map<String, Object> digest(Element node) {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, Object> attributes = new HashMap<String, Object>();
        StringBuilder textContent = new StringBuilder();
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            switch (child.getNodeType()) {
                case Node.ELEMENT_NODE:{
                    Element el = (Element)child;
                    String type = schema != null ? schema.typeOf(el.getNodeName()) : null;
                    if (type == null || "xs:complexType".equals(type))
                        result.put(el.getTagName(), digest(el));
                    else if ("xs:boolean".equals(type))
                        result.put(el.getTagName(), "true".equals(el.getTextContent().trim()));
                    else if ("xs:int".equals(type))
                        result.put(el.getTagName(), Integer.parseInt(el.getTextContent().trim()));
                    else if ("xs:int".equals(type))
                        result.put(el.getTagName(), Integer.parseInt(el.getTextContent().trim()));
                    else if ("xs:double".equals(type))
                        result.put(el.getTagName(), Double.parseDouble(el.getTextContent().trim()));
                    else // default to string, but there are other that we could support, e.g. dates etc.
                        result.put(el.getTagName(), el.getTextContent());
                    break;
                }
                case Node.ATTRIBUTE_NODE: {
                    Attr attr = (Attr)node;
                    attributes.put(attr.getName(), attr.getValue());
                    break;
                }
                default:
                    textContent.append(child.getTextContent());
            }
        }
        result.putAll(attributes);
        if (result.isEmpty())
            result.put("textContent", textContent.toString());
        return result;
    }

    Stream<Element> toXmlNodes(Reader reader) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(new InputSource(reader)); 
            final NodeList list = doc.getElementsByTagName(this.element); 
            Iterable<Element> iterable = () -> new Iterator<Element>() {
                int current = 0;
                public boolean hasNext() { return current < list.getLength(); }
                public Element next() { return (Element)list.item(current++); }
                public void remove() { throw new UnsupportedOperationException(); }
            };
            return StreamSupport.stream(iterable.spliterator(), false);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Convert a file into a Reader
     * @param file file to be converted
     * @return Json object representing the file, empty if problem reading file
     */
    private InputStreamReader asReader(File file){
        try {
            return new InputStreamReader(new FileInputStream(file), Charset.defaultCharset());
        } catch (IOException e){
            throw new RuntimeException("Problem reading input");
        }
    }

    /**
     * Filter that will only accept XML files with the .xml extension
     */
    private final FilenameFilter xmlFiles = (dir, name) -> name.toLowerCase().endsWith(".xml");
}
