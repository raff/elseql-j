elseql-j
========

You know, for Query - java edition

### USAGE

    usage: elseql [--host=host:port] [--csv|--json|--xml|--native] "query"

where:

    --host: ES server name and port (default localhost:9200)
    --csv: format results as CSV (one line per record, comma separated fields)
    --json: format results as JSON (array of JSON objects)
    --xml: format results as XML (list of items)
    --native: return ES result as-is

### ES SERVER
* By default elseql will try to connect to localhost:9200
* host and port can be specified on the command line using the "--host" option (see USAGE)
* host and port can also be specified via the environment variable ELSEQL_HOST

* If the ElasticSearch server is behind a firewall (or listening only for localhost connections) but the machine is running an SSH daemon, elseql also accept "tunnel" connections (it will tunnel ES HTTP requests over SSH) by specifying the host URL as 'tunnel:user:password@remotehost:remoteport'.
This will create a tunnell from localhost:remoteport to remotehost:remoteport using the appropriate credentials.
For now only password authentication is supported.
For simplification currently you cannot specify a local port different than the remote port

### QUERY

    SELECT {fields|*}
        [FACETS facet-fields]
        [SCRIPT script-field = 'script']
        FROM index
        [WHERE where-condition]
        [FILTER filter-condition]
        [ORDERY BY order-fields]
        [LIMIT [start,] count]

where:
    fields: '*' or comma-separated list of field names to be returned

    facet-fields: comma-separated list of fields to execute a facet query on

    script-field: name of script field, to be used in select clause
    script: ElasticSearch script

    index: index to query

    where-condition:
        {field-name} [ = != > >= < <= ] {value}
        {field-name} LIKE {value}
        {field-name} IN (value1, value2, ...)
        {field-name} BETWEEN {min-value} AND {max-value}
        NOT {where-condition}
        {where-condition} AND {where-condition}
        {where-condition} OR {where-condition}

    or where-condition:
        'query in Lucene syntax'

    filter-condition: 
        QUERY {where-condition} - query filter, same syntax as where condition
        EXIST {field-name}      - exists field filter
        MISSING {field.name}    - missing field filter

    order-fields: comma-separated list of {field-name} [ASC | DESC]

    start: start index for pagination
    count: maximum number of returned results
