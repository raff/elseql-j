package org.aromatic.elseql;

import java.util.*;

public class ElseSearch
{
    String endpoint;

    public ElseSearch(String url) {
        this.endpoint = url;
    }

    public void search(String queryString, Util.Format format, boolean streaming, boolean debug) throws Exception {

        ElseParser parser = new ElseParser(queryString);
        ElseParser.Query query = null;

        try {
            query = parser.getQuery();
        } catch(ElseParser.ParseException e) {
            System.out.println("syntax error: " + e.getMessage());
            return;
        }

        Util.Json data = new Util.Json();

        if (query.whereExpr != null) {
            data.put("query", new Util.Json()
                    .put("query_string", new Util.Json()
                        .put("query", query.whereExpr.toQueryString())
                        .put("default_operator", "AND")));
        } else {
            data.put("query", new Util.Json().put("match_all", new Util.Json()));
        }

        if (query.filterExpr != null) {
	    Util.Json filter = new Util.Json();

	    if (query.filterExpr.isExistsExpression())
                filter.put("exists", new Util.Json()
		      .put("field", (String) query.filterExpr.getOperand()));

	    else if (query.filterExpr.isMissingExpression())
                filter.put("missing", new Util.Json()
		      .put("field", (String) query.filterExpr.getOperand()));

	    else
                filter.put("query", new Util.Json()
                      .put("query_string", new Util.Json()
                        .put("query", query.filterExpr.toQueryString())
                        .put("default_operator", "AND")));

	    data.put("filter", filter);
        }

        if (query.facetList != null) {
            Util.Json facets = new Util.Json();

            for (String f : query.facetList) {
                facets.put(f, new Util.Json().put("terms", new Util.Json().put("field", f)));
            }

            data.put("facets", facets);
        }

        if (query.script != null) {
	    data.put("script_fields", new Util.Json()
	    	.put(query.script.name, new Util.Json()
		    .put("script", (String) query.script.value)));
        }

        if (query.selectList != null) {
            data.put("fields", query.selectList);
        }

        if (query.orderList != null) {
            data.putNVList("sort", query.orderList);
        }

        data.put("from", query.from);
        data.put("size", query.size);

        String url = endpoint + "/" + query.index + "/_search";

	if (debug) {
	   System.out.println("REQUEST: " + url);
	   System.out.println(data.toPrettyString());
	   System.out.println();
	}

        Util.Json result = Util.get_json(url, data.toString());

        StringBuilder info = new StringBuilder();

	if (format == Util.Format.NATIVE || debug) {
	    System.out.println(result.toPrettyString());
	    return;
	}

        if (result.has("status")) { // some error has occurred
            System.out.println("status: " + result.get("status"));
            System.out.println("error: " + result.get("error"));
        } else {
            info.append("took: " + result.get("took"));
            info.append(", timed_out: " + result.get("timed_out"));
        }

        if (result.has("hits")) {
            Util.Json hits = result.get("hits");

            info.append(", total: " + hits.get("total"));

            hits = hits.get("hits");
            info.append(", retrieved: " + hits.size());

            String field_names[] = null;

            if (query.selectList != null)
                field_names = query.selectList.toArray(new String[0]);
            else if (hits.size() > 0)
                field_names = hits.get(0).get("_source").keySet().toArray(new String[0]);

	    if (format == Util.Format.CSV) {
	        System.out.println(Util.join(",", field_names));
	    }

            if (!streaming) {
	        String listMarker = Util.startList(format);
	        if (listMarker != null)
                    System.out.println(listMarker);
	    }

            int size = hits.size();

            for (int i=0; i < size; i++) {
                Util.Json r = hits.get(i);

                Util.Json fields = null;

                if (r.has("fields"))
                    fields = r.get("fields");
                else if (r.has("_source"))
                    fields = r.get("_source");

                if (fields != null) {
		    if (format == Util.Format.CSV) {
		        System.out.println(fields.toCSV(field_names));
		    }

		    else if (format == Util.Format.JSON) {
			System.out.print(fields);
                        System.out.println((i+1) < size ? "," : "");
		    }

		    else {
			System.out.println(fields.toXML(query.from + i));
		    }
                } else
                    System.out.println(r);
            }

            if (!streaming) {
	        String listMarker = Util.endList(format);
	        if (listMarker != null)
                    System.out.println(listMarker);
	    }

            System.out.println();
            System.out.println(info.toString());
            System.out.println();
        }

        if (result.has("facets")) {
            Util.Json facets = result.get("facets");

            for (String f : facets.keySet()) {
                Util.Json facet = facets.get(f);

                System.out.println("facet " + f);
                System.out.println("  total: " + facet.get("total"));
                System.out.println("  other: " + facet.get("other"));
                System.out.println("  missing: " + facet.get("missing"));
                System.out.println("  terms:");

                Util.Json terms = facet.get("terms");
                for (int i=0; i < terms.size(); i++)
                    System.out.println("    " + terms.get(i));
            }

            System.out.println();
        }
    }

    public static void usage(String error) {

	if (error != null)
	    System.out.println(error);

        System.out.println("usage: elseql [--host=host:port] [--csv|--json|--xml|--native] \"query\"");
	System.exit(error==null ? 0 : 1);
    }

    public static void main(String args[]) throws Exception {

        String host = "http://localhost:9200";
        String query = null;
	Util.Format format = Util.Format.CSV;
	boolean debug = false;
	boolean streaming = false;

        if (System.getenv().containsKey("ELSEQL_HOST"))
            host = System.getenv("ELSEQL_HOST");

        int argc = 0;

        for (argc=0; argc < args.length; argc++) {
            if (! args[argc].startsWith("-"))
		break;

	    if (args[argc].startsWith("--host=")) {
	        host = args[argc].substring(7);
	    }

	    else if (args[argc].equals("--native")) {
		format = Util.Format.NATIVE;
	    }

	    else if (args[argc].equals("--csv")) {
		format = Util.Format.CSV;
	    }

	    else if (args[argc].equals("--json")) {
		format = Util.Format.JSON;
	    }

	    else if (args[argc].equals("--xml")) {
		format = Util.Format.XML;
	    }

	    else if (args[argc].equals("--stream")) {
		streaming = true;
	    }

	    else if (args[argc].equals("--debug")) {
		debug = true;
	    }

	    else {
                String message = args[argc].equals("--help") ? null : "invalid option: " + args[argc];
		usage(message);
	    }
	}

        try {
            if (host.startsWith("tunnel:")) {
                // tunnel:user@remotehost:remoteport
                int port = Util.startTunnel(host.substring(7));

                host = "http://localhost:" + port;
            }

            query = Util.join(" ", args, argc);

            ElseSearch search = new ElseSearch(host);
            search.search(query, format, streaming, debug);
	} catch(Exception e) {
	    System.out.println("ERROR " + e);
        } finally {
            Util.stopTunnel();
            System.exit(0);
        }
    }
}
