package org.aromatic.elseql;

import java.io.*;
import java.net.*;
import java.util.*;

import com.google.gson.*;
import com.jcraft.jsch.*;

public class Util
{
    public static boolean DEBUG = true;

        /*
         * A container for name/value pair objects
         */
    public static class NameValue {
        String name;
        Object value;

        public NameValue(String name, Object value) {
            this.name = name;
            this.value = value;
        }


        public String toString() {
            return name + ":" + value;
        }
    }

        /*
         * A simple wrapper for Gson objects
         */
    public static class Json {

        JsonElement jele;

        public Json() {
            jele = new JsonObject();
        }

        public Json(JsonElement source) {
            jele = source;
        }

        public String toPrettyString() {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(jele);
        }

        public String toString() {
            return jele.toString();
        }

        public String toXML() {
            return elementToXML(jele);
        }

        public String toXML(int index) {
            return "<item index=\"" + index + "\">"
                + elementToXML(jele)
                + "</item>";
        }

        public static String elementToXML(JsonElement jele) {
            //
            // null are empty values
            //
            if (jele==null || jele.isJsonNull())
                return "";
            
            if (jele.isJsonPrimitive()) {
                JsonPrimitive p = jele.getAsJsonPrimitive();

                if (p.isString())
                    return p.getAsString().replace("&", "&amp;").replace("<", "&gt;");

                //
                // other primitives are returned as-is
                //
                else
                    return p.toString();
            }

            StringBuilder sb = new StringBuilder();

            if (jele.isJsonObject()) {
                sb.append("<object>");

                for (Map.Entry<String, JsonElement> e : jele.getAsJsonObject().entrySet()) {
                    sb.append("<property name=\"" + e.getKey() + "\">");
                    sb.append(elementToXML(e.getValue()));
                    sb.append("</property>");
                }

                sb.append("</object>");
            }

            else if (jele.isJsonArray()) {
                sb.append(startList(Format.XML));

                int i=0;

                for (JsonElement e : jele.getAsJsonArray()) {
                    sb.append("<item index=\"" + i + "\">");
                    sb.append(elementToXML(e));
                    sb.append("</item>");

                    i++;
                }

                sb.append(endList(Format.XML));
            }

            else {
                sb.append("<unknown>");
                sb.append(jele.toString());
                sb.append("</unknown>");
            }

            return sb.toString();
        }

        public String toCSV() {
            //
            // null are empty values
            //
            if (jele==null || jele.isJsonNull())
                return "";
            
            if (jele.isJsonPrimitive()) {
                //
                // strings are CSV-escaped
                //
                    if (jele.getAsJsonPrimitive().isString())
                    return "\"" + jele.getAsJsonPrimitive().getAsString().replace("\"", "\"\"") + "\"";


                //
                // other primitives are returned as-id
                //
                else
                    return jele.toString();
            }

            //
            // non-primitives are fully CSV-escaped so they can be converted back to json blobs
            //
            return "\"" + jele.toString().replace("\"", "\"\"") + "\"";
        }

        public String toCSV(String field_names[]) {
            ArrayList<String> values = new ArrayList<String>(field_names.length);

            for (String name : field_names)
                values.add(this.get(name).toCSV());

            return join(",", values);
        }

        public Json putNVList(String name, List<NameValue> value) {
            JsonArray jarray = new JsonArray();

            for (NameValue nv : value) {
                JsonObject jnv = new JsonObject();
                jnv.addProperty(nv.name, nv.value.toString());
                jarray.add(jnv);
            }

            jele.getAsJsonObject().add(name, jarray);
            return this;
        }

        public Json put(String name, List<?> value) {
            JsonArray jarray = new JsonArray();

            for (Object v : value)
                jarray.add(new JsonPrimitive(v.toString()));

            jele.getAsJsonObject().add(name, jarray);
            return this;
        }

        public Json put(String name, Json value) {
            jele.getAsJsonObject().add(name, value.jele);
            return this;
        }

        public Json put(String name, Boolean value) {
            jele.getAsJsonObject().addProperty(name, value);
            return this;
        }

        public Json put(String name, Integer value) {
            jele.getAsJsonObject().addProperty(name, value);
            return this;
        }

        public Json put(String name, Long value) {
            jele.getAsJsonObject().addProperty(name, value);
            return this;
        }

        public Json put(String name, Double value) {
            jele.getAsJsonObject().addProperty(name, value);
            return this;
        }

        public Json put(String name, String value) {
            jele.getAsJsonObject().addProperty(name, value);
            return this;
        }

        public boolean has(String name) {
            return jele.getAsJsonObject().has(name);
        }

        public Set<String> keySet() {
            Set<String> keySet = new HashSet<String>();

            for (Map.Entry<String, JsonElement> e : jele.getAsJsonObject().entrySet()) {
                keySet.add(e.getKey());
            }

            return keySet;
        }

        public Json get(String name) {
            return new Json(jele.getAsJsonObject().get(name));
        }

        public boolean getBoolean(String name) {
            return jele.getAsJsonObject().get(name).getAsBoolean();
        }

        public int getInt(String name) {
            return jele.getAsJsonObject().get(name).getAsInt();
        }

        public long getLong(String name) {
            return jele.getAsJsonObject().get(name).getAsLong();
        }

        public double getDouble(String name) {
            return jele.getAsJsonObject().get(name).getAsDouble();
        }

        public String getString(String name) {
            return jele.getAsJsonObject().get(name).getAsString();
        }

        public Json get(int index) {
            return new Json(jele.getAsJsonArray().get(index));
        }

        public int size() {
            return jele.getAsJsonArray().size();
        }
    }

    /*
     * An exception class for HTTP errors where the body may be meaningful
     */
    public static class HttpError extends Exception {

        public int code;
        public String contentType;
        public String body;

        public HttpError(int code, String contentType, String body) {
            super("HttpError " + code);
            this.code = code;
            this.contentType = contentType;
            this.body = body;
        }
    }

    public static final String readStream(InputStream stream) throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        StringBuilder sb = new StringBuilder();
        String line;

        while (null != (line = reader.readLine()))
            sb.append(line).append('\n');

        return sb.toString();
    }

    public static final String GET = "GET";
    public static final String POST = "POST";

    public static Reader http_request(String method, String url, String data, Map<String, String> headers)
        throws Exception
    {
        URL req = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) req.openConnection();

        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet())
                connection.addRequestProperty(header.getKey(), header.getValue());
        }

        connection.setRequestMethod(method);

        if (DEBUG)
            System.out.println("DEBUG request " + connection);

        if (data != null) {
            connection.setDoOutput(true);
            Writer writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(data);
            writer.flush();
            writer.close();
        }

        if (DEBUG)
            System.out.println("DEBUG connecting...");

        connection.connect();
        int responseCode = connection.getResponseCode();

        if (DEBUG)
            System.out.println("DEBUG response " + connection);

        if (responseCode == 200)
            return new InputStreamReader(connection.getInputStream());

        throw new HttpError(responseCode, connection.getContentType(), readStream(connection.getErrorStream()));
    }

    public static Reader http_get(String url) throws Exception {
        return http_request(GET, url, null, null);
    }

    public static Reader http_get(String url, String data) throws Exception {
        return http_request(GET, url, data, null);
    }

    public static Reader http_post(String url, String data) throws Exception {
        return http_request(POST, url, data, null);
    }

    public static Json get_json(String url, String data) throws Exception {

        JsonParser parser = new JsonParser();

        try {
            Reader reader = http_get(url, data);
            return new Json(parser.parse(reader).getAsJsonObject());
        } catch(HttpError error) {
            if (DEBUG) {
                System.out.println("ERROR code " + error.code);
                System.out.println("ERROR contentType " + error.contentType);
                System.out.println("ERROR body " + error.body);
            }

            if (error.contentType != null && error.contentType.contains("json") && error.body != null)
                return new Json(parser.parse(error.body).getAsJsonObject());

            throw error;
        }
    }

    public static String join(String sep, Collection<String> args) {
            return join(sep, args.toArray(new String[0]), 0);
    }

    public static String join(String sep, String args[]) {
            return join(sep, args, 0);
    }

    public static String join(String sep, String args[], int start) {

        if (args.length==0 || args.length <= start)
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append(args[start]);

        for (int i=start+1; i < args.length; i++) {
            sb.append(sep).append(args[i]);
        }

        return sb.toString();
    }

    public enum Format {
        NATIVE, CSV, JSON, XML
    }

    public static String startList(Format format) {
        switch(format)
        {
        case XML:
            return "<list>";

        case JSON:
            return "[";

        default:
            return null;
        }
    }

    public static String endList(Format format) {
        switch(format)
        {
        case XML:
            return "</list>";

        case JSON:
            return "]";

        default:
            return null;
        }
    }

        /*
         * Create an SSH Tunnel
         * 
         * user: ssh user
         * host: ssh host
         * lport: local port
         * rhost: remote host
         * rport: remote port
         */

    public static Session tunnelSession = null;

        // user@host:port
    public static int startTunnel(String connectionString) throws Exception
    {
        String parts[];

        String user = System.getProperty("user.name");
        String password = null;

        parts = connectionString.split("@", 2);
        if (parts.length > 1) {
            user = parts[0];
            connectionString = parts[1];

            parts = user.split(":", 2);
            if (parts.length > 1) {
                user = parts[0];
                password = parts[1];
            }
        }
        
        parts = connectionString.split(":", 2);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        return startTunnel(user, password, host, port, "127.0.0.1", port);
    }

    public static int startTunnel(String user, String password, String host, int lport, String rhost, int rport) throws Exception
    {
        stopTunnel();

        JSch jsch = new JSch();
        Session tunnelSession = jsch.getSession(user, host);
        tunnelSession.setPassword(password);

        Properties config = new Properties();
        config.put("StrictHostKeyChecking","no");
        tunnelSession.setConfig(config);
        tunnelSession.connect();

        return tunnelSession.setPortForwardingL(lport, rhost, rport);
    }

    public static void stopTunnel() {
        if (tunnelSession != null) {
            tunnelSession.disconnect();
            tunnelSession = null;
        }
    }
}
