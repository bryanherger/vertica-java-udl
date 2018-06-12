package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// XML import references:
// https://viralpatel.net/blogs/java-xml-xpath-tutorial-parse-xml/
// https://www.mkyong.com/java/how-to-read-xml-file-in-java-dom-parser/

public class XmlParser extends UDParser {
    private RejectedRecord rejectedRecord;
    private String recordTag = "//item"; // default value
    private DocumentBuilder builder;
    private ServerInterface serverInterface;
    private Map<String, Object> map = new HashMap<>();
    private Set<String> fields = new HashSet<>();

    public XmlParser(ServerInterface si) {
        serverInterface = si;
        String docTag = null;
        try {
            docTag = si.getParamReader().getString("document");
        } catch (Exception e) {
            for (String s : si.getParamReader().getParamNames()) {
                serverInterface.log("param = %s", s);
            }
        }
        if (docTag != null) {
            serverInterface.log("docTag = %s", docTag);
            recordTag = "//" + docTag;
        } else {
            serverInterface.log("docTag is null, using 'item'");
        }
        DocumentBuilderFactory builderFactory =
                DocumentBuilderFactory.newInstance();
        builder = null;
        try {
            builder = builderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    private ByteBuffer consumeNextLine(DataBuffer input, InputState inputState) {
        switch (inputState) {
            case END_OF_FILE:
            case END_OF_CHUNK:
                ByteBuffer rv = ByteBuffer.wrap(input.buf,
                        input.offset, input.buf.length - input.offset);
                input.offset = input.buf.length;
                return rv;
            case OK:
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unknown InputState: " + inputState.toString());
        }
    }

    private void flattenXML(String pathTo, NodeList theseNodes) {
        for (int i = 0; i < theseNodes.getLength(); i++) {
            Node ii = theseNodes.item(i);
            if (ii.hasChildNodes()) {
                flattenXML(pathTo+ii.getNodeName()+"_", ii.getChildNodes());
                if (ii.hasAttributes()) {
					serverInterface.log("1 adding attributes: %s", pathTo+ii.getNodeName());
					NamedNodeMap nodeMap = ii.getAttributes();
					for (int jj = 0; jj < nodeMap.getLength(); jj++) {
						Node an = nodeMap.item(jj);
						String aname = pathTo+ii.getNodeName()+"_attr_"+an.getNodeName();
            			String avalue = an.getNodeValue();
						serverInterface.log("attr %d: %s, %s",jj,aname,avalue);
						map.put(aname, avalue);
						fields.add(aname);
    				}
                }
            } else if (ii.getTextContent()!=null && !"".equals(ii.getTextContent().trim())) {
				String field = (pathTo+ii.getNodeName()).replace("#","");
				serverInterface.log("adding flatten: %s, %s", field, ii.getTextContent());
				map.put(field, ii.getTextContent());
				fields.add(field);
                if (ii.hasAttributes()) {
					serverInterface.log("2 adding attributes: %s", field);
                }
            }
        }
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input,
                               InputState inputState) throws UdfException, DestroyInvocation {
        clearReject();
        StreamWriter output = getStreamWriter();

        while (input.offset < input.buf.length) {
            ByteBuffer lineBytes = consumeNextLine(input, inputState);

            if (lineBytes == null) {
                return StreamState.INPUT_NEEDED;
            }

            serverInterface.log("Read bytes: %s", new String(lineBytes.array()));

            Document document;
            try {
                document = builder.parse(
                        new ByteArrayInputStream(lineBytes.array())
                );
            } catch (SAXException | IOException e) {
                e.printStackTrace();
                throw new UdfException(42, e);
            }
            XPath xPath = XPathFactory.newInstance().newXPath();
            NodeList nodeList;
            try {
                nodeList = (NodeList) xPath.compile(recordTag).evaluate(document, XPathConstants.NODESET);
            } catch (XPathExpressionException e) {
                throw new UdfException(42, e);
            }
            serverInterface.log("nodeList: %d", nodeList.getLength());
            for (int i = 0; i < nodeList.getLength(); i++) {
                serverInterface.log("Parsing node %d", i);
                NodeList docNodeList = nodeList.item(i).getChildNodes();
                map.clear();
	            flattenXML("", docNodeList);
                /*for (int j = 0; j < docNodeList.getLength(); j++) {
                    Node docNode = docNodeList.item(j);
                    try {
                        //serverInterface.log("reference Name: %s, Value: %s", docNode.getNodeName(), docNode.getTextContent());
                        //map.put(docNode.getNodeName(), docNode.getTextContent());
                    } catch (Exception e) {
                        serverInterface.log("XML parse exception (will try to continue): %s" + e.getMessage());
                    }
                }*/
                output.setRowFromMap(map);
                output.next();
            }
        }
        String DDL = "CREATE TABLE forThisXmlDoc (";
        boolean first = false;
        for (String field : fields) {
            if (first) {
                DDL = DDL + ", ";
            }
            DDL = DDL + field + " VARCHAR";
            first = true;
        }
        DDL = DDL + ");";
        serverInterface.log("DDL for this XML file is: %s", DDL);

        return StreamState.DONE;
    }

    private void clearReject() {
        rejectedRecord = null;
    }

    private void setReject(String line, Exception ex) {
        rejectedRecord = new RejectedRecord(ex.getMessage(), line.toCharArray());
    }

    @Override
    public RejectedRecord getRejectedRecord() throws UdfException {
        return rejectedRecord;
    }
}
