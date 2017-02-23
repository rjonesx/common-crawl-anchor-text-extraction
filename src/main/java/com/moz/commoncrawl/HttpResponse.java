package com.moz.commoncrawl;

// JDK imports
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/** An HTTP response - borrowed from Nutch */
public class HttpResponse {

    public final static String CONTENT_LENGTH = "Content-Length";

    private byte[] content;
    private int code;
    private Map<String, String> headers = new HashMap();

    public static final int BUFFER_SIZE = 8 * 1024;

    public HttpResponse(byte[] response) throws IOException {

        PushbackInputStream in = // process response
                new PushbackInputStream(new ByteArrayInputStream(response),
                        BUFFER_SIZE);

        StringBuilder line = new StringBuilder();

        boolean haveSeenNonContinueStatus = false;
        while (!haveSeenNonContinueStatus) {
            // parse status code line
            this.code = parseStatusLine(in, line);
            // parse headers
            parseHeaders(in, line);
            haveSeenNonContinueStatus = code != 100; // 100 is "Continue"
        }

        readPlainContent(in);
    }

    public int getCode() {
        return code;
    }

    public String getHeader(String name) {
        return headers.get(name);
    }

    public Map getHeaders() {
        return headers;
    }

    public byte[] getContent() {
        return content;
    }

    /*
     * ------------------------- * <implementation:Response> *
     * -------------------------
     */

    private void readPlainContent(InputStream in) throws IOException {

        int contentLength = Integer.MAX_VALUE; // get content length
        String contentLengthString = headers.get(CONTENT_LENGTH);
        if (StringUtils.isNotBlank(contentLengthString)) {
            contentLengthString = contentLengthString.trim();
            try {
                contentLength = Integer.parseInt(contentLengthString);
            } catch (NumberFormatException e) {
                throw new IOException(
                        "bad content length: " + contentLengthString);
            }
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
        byte[] bytes = new byte[BUFFER_SIZE];
        int length = 0; // read content
        for (int i = in.read(bytes); i != -1; i = in.read(bytes)) {

            out.write(bytes, 0, i);
            length += i;
            if (length >= contentLength)
                break;
        }
        content = out.toByteArray();
    }

    private int parseStatusLine(PushbackInputStream in, StringBuilder line)
            throws IOException {
        // skip first character if "\n"
        if (peek(in) == '\n') {
            in.read();
        }
        readLine(in, line, false);

        int codeStart = line.indexOf(" ");
        int codeEnd = line.indexOf(" ", codeStart + 1);

        // handle lines with no plaintext result code, ie:
        // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
        if (codeEnd == -1)
            codeEnd = line.length();

        int code;
        try {
            code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
        } catch (NumberFormatException e) {
            throw new IOException(
                    "bad status line '" + line + "': " + e.getMessage(), e);
        }

        return code;
    }

    private void processHeaderLine(StringBuilder line) throws IOException {

        int colonIndex = line.indexOf(":"); // key is up to colon
        if (colonIndex == -1) {
            int i;
            for (i = 0; i < line.length(); i++)
                if (!Character.isWhitespace(line.charAt(i)))
                    break;
            if (i == line.length())
                return;
            throw new IOException("No colon in header:" + line);
        }
        String key = line.substring(0, colonIndex);

        int valueStart = colonIndex + 1; // skip whitespace
        while (valueStart < line.length()) {
            int c = line.charAt(valueStart);
            if (c != ' ' && c != '\t')
                break;
            valueStart++;
        }
        String value = line.substring(valueStart);
        headers.put(key, value);
    }

    // Adds headers to our headers Metadata
    private void parseHeaders(PushbackInputStream in, StringBuilder line)
            throws IOException {

        while (readLine(in, line, true) != 0) {

            // handle HTTP responses with missing blank line after headers
            int pos;
            if (((pos = line.indexOf("<!DOCTYPE")) != -1)
                    || ((pos = line.indexOf("<HTML")) != -1)
                    || ((pos = line.indexOf("<html")) != -1)) {

                in.unread(line.substring(pos).getBytes("UTF-8"));
                line.setLength(pos);

                try {
                    // TODO: (CM) We don't know the header names here
                    // since we're just handling them generically. It would
                    // be nice to provide some sort of mapping function here
                    // for the returned header names to the standard metadata
                    // names in the ParseData class
                    processHeaderLine(line);
                } catch (Exception e) {

                }
                return;
            }

            processHeaderLine(line);
        }
    }

    private static int readLine(PushbackInputStream in, StringBuilder line,
            boolean allowContinuedLine) throws IOException {
        line.setLength(0);
        for (int c = in.read(); c != -1; c = in.read()) {
            switch (c) {
            case '\r':
                if (peek(in) == '\n') {
                    in.read();
                }
            case '\n':
                if (line.length() > 0) {
                    // at EOL -- check for continued line if the current
                    // (possibly continued) line wasn't blank
                    if (allowContinuedLine)
                        switch (peek(in)) {
                        case ' ':
                        case '\t': // line is continued
                            in.read();
                            continue;
                        }
                }
                return line.length(); // else complete
            default:
                line.append((char) c);
            }
        }
        throw new EOFException();
    }

    private static int peek(PushbackInputStream in) throws IOException {
        int value = in.read();
        in.unread(value);
        return value;
    }

}