package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import org.w3c.dom.Document;
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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// FIX import references:

public class FixParser extends UDParser {
    private RejectedRecord rejectedRecord;
    private ServerInterface serverInterface;
    private byte lineTerm;
    private Map<String, Object> map = new HashMap<>();
    private Set<String> fields = new HashSet<>();

    public FixParser(ServerInterface si) {
        serverInterface = si;
        byte[] ltBytes = "\n".getBytes(Charset.forName("US-ASCII"));
        if (ltBytes.length != 1) {
            throw new IllegalArgumentException("Terminating character is multi-byte");
        }
        lineTerm = ltBytes[0];
    }

    private static int byteArrayIndexOf(byte[] buf, int offset, byte search) {
        for (int i=offset; i<buf.length; i++) {
            if (buf[i] == search) {
                return i;
            }
        }

        return -1;
    }

    private ByteBuffer consumeNextLine(DataBuffer input, InputState inputState) {
        int lineEnd = byteArrayIndexOf(input.buf, input.offset, lineTerm);

        if (lineEnd == -1) {
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

        ByteBuffer rv = ByteBuffer.wrap(input.buf,
                input.offset, lineEnd - input.offset);
        input.offset = lineEnd+1; // skip the lineTerm byte
        return rv;
    }

    private void parseFIX(String fixRecord) {
		serverInterface.log("Got record: %s", fixRecord);
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
			String fixRecord = new String(lineBytes.array());
			parseFIX(fixRecord);
			output.setRowFromMap(map);
			output.next();
        }
        String DDL = "CREATE TABLE forThisFixDoc (";
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
