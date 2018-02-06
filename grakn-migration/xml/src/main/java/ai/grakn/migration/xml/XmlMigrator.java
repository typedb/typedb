/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.migration.base.MigrationCLI;
import ai.grakn.migration.xml.XmlSchema.TypeInfo;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;

/**
 * Migrator for migrating XML data into Grakn instances
 * @author boris
 */
public class XmlMigrator implements AutoCloseable {
    
    public static void main(String[] args) {
        try {
            MigrationCLI.init(args, XmlMigrationOptions::new).stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(XmlMigrator::runXml);
        } catch (IllegalArgumentException e){
            System.err.println(e.getMessage());
        }
    }
    
    public static void runXml(XmlMigrationOptions options){
        File xmlDataFile = new File(options.getInput());
        File xmlTemplateFile = new File(options.getTemplate());

        if(!xmlDataFile.exists()){
            throw new IllegalArgumentException("Cannot find file: " + options.getInput());
        }

        if(!xmlTemplateFile.exists() || xmlTemplateFile.isDirectory()){
            throw new IllegalArgumentException("Cannot find file: " + options.getTemplate());
        }

        try(XmlMigrator xmlMigrator = new XmlMigrator(xmlDataFile)){
            if (options.getElement() != null) {
                xmlMigrator.element(options.getElement());
            } else {
                throw new IllegalArgumentException("Please specify XML element for the top-level data item.");
            }

            if (options.getSchemaFile() != null) {
                xmlMigrator.schema(new XmlSchema().read(new File(options.getSchemaFile())));
            }

            MigrationCLI.loadOrPrint(xmlTemplateFile, xmlMigrator.convert(), options);
        }
    }
    
    private XmlSchema schema;
    private final Set<Reader> readers;
    private String element;
    
    
    /**
     * Construct a XmlMigrator to migrate data in the given file or dir
     * @param xmlFileOrDir either a XML file or a directory containing XML files
     */
    public XmlMigrator(File xmlFileOrDir) {
        File[] files = {xmlFileOrDir};
        if(xmlFileOrDir.isDirectory()){
            files = xmlFileOrDir.listFiles(xmlFiles);
        }
        this.readers = Stream.of(files).map(this::asReader).collect(toSet());
        this.schema = new XmlSchema();
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
     * Migrate each of the given XML objects as a nested Map structure
     */
    public Stream<Map<String, Object>> convert(){
        return readers.stream()
                .flatMap(this::toXmlNodes)
                .map(this::digest)
                .map(data -> (Map<String, Object>)data);
                //.map(data -> { System.out.println(data); return data; } );
    }

    /**
     * Close the readers
     * @throws Exception
     */
    public void close() {
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
     * @param node XML element (a tag) to convert
     * @return A String containing the text content of the element or a Map with its nested elements.
     */
    Map<String, Object> digest(Element node) {
        Map<String, Object> result = new HashMap<String, Object>();

        StringBuilder textContent = new StringBuilder();
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            switch (child.getNodeType()) {
                case Node.ELEMENT_NODE:{
                    Element el = (Element)child;
                    Object value = null;
                    TypeInfo type = schema.typeOf(el.getNodeName());
                    if ("xs:complexType".equals(type.name())) {
                        value = digest(el);
                    }
                    else if ("xs:boolean".equals(type.name())) {
                        value = "true".equals(el.getTextContent().trim());
                    }
                    else if ("xs:int".equals(type.name())) {
                       value = Integer.parseInt(el.getTextContent().trim());
                    }
                    else if ("xs:double".equals(type.name())) {
                        value = Double.parseDouble(el.getTextContent().trim());
                    }
                    else { // default to string, but there are other that we could support, e.g. dates etc.
                        value = el.getTextContent();
                    }
                    if (type.cardinality() > 1) {
                        @SuppressWarnings("unchecked")
                        List<Object> allValues = (List<Object>)result.get(el.getTagName());    
                        if (allValues == null) {
                            allValues = new ArrayList<Object>();
                            result.put(el.getTagName(),  allValues);
                        }
                        allValues.add(value);
                    }
                    else {
                        result.put(el.getTagName(), value);
                    }
                    break;
                }
                default:
                    textContent.append(child.getTextContent().trim());
            }
        }

        NamedNodeMap attributes = node.getAttributes();
        for(int i = 0; i < attributes.getLength(); i++){
            Attr attr = (Attr) attributes.item(i);
            result.put("~" + attr.getName(), attr.getValue());
        }

        if(textContent.length() > 0) {
            result.put("textContent", textContent.toString());
        }

        return result;
    }

    Stream<Element> toXmlNodes(Reader reader) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(new InputSource(reader)); 
            final NodeList list = doc.getElementsByTagName(this.element);
            Iterable<Element> iterable = () -> new ElementIterator(list);
            return StreamSupport.stream(iterable.spliterator(), false);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new RuntimeException(e);
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

    private static class ElementIterator implements Iterator<Element> {
        private final NodeList list;
        int current;

        ElementIterator(NodeList list) {
            this.list = list;
            current = 0;
        }

        public boolean hasNext() { return current < list.getLength(); }

        public Element next() {
            Element elem = (Element) list.item(current++);
            if (elem == null) {
                throw new NoSuchElementException();
            } else {
                return elem;
            }
        }

        public void remove() { throw new UnsupportedOperationException(); }
    }
}