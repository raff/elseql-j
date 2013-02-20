package org.aromatic.elseql;

import java.io.*;
import java.util.*;

/* SELECT a,b,c FACETS d,e,f FROM t WHERE expr FILTER expr ORDER BY g,h,i LIMIT n,m */

public class ElseParser
{
    public static final boolean DEBUG = false;

    public static final char ID_SEPARATOR = '.';
    public static final char LIST_SEPARATOR = ',';
    public static final char ALL_FIELDS = '*';

        /*
         * reserved keywords
         */
    private enum Keyword {
        SELECT,
        FACETS,
        SCRIPT,
        FROM,
        WHERE,
        FILTER,
        EXIST,
        MISSING,
        ORDER,
        BY,
        LIMIT,
        ASC,
        DESC,
        AND,
        OR,
        NOT,
        IN,
        BETWEEN
        ;

        private String token = null;

        private Keyword() {
        }

        private Keyword(String token) {
            this.token = token;
        }
    };

        /*
         * This is the output of a parsed statement
         */
    public static class Query {
        public List<String> selectList = null;
        public List<String> facetList = null;
        public Util.NameValue script = null;
        public String index = null;
        public Expression whereExpr = null;
        public Expression filterExpr = null;
        public List<Util.NameValue> orderList = null;
        public int from = 0;
        public int size = 10;

        public String toQueryString(Expression expr) {
            if (expr == null)
                return null;

            return expr.toQueryString();
        }

        public String toString() {
            return 
                "select " + selectList
                + "\nfacet " + facetList
                + "\nindex " + index
                + "\nwhere " + toQueryString(whereExpr)
                + "\nfilter " + toQueryString(filterExpr)
                + "\norder " + orderList
                + "\nfrom " + from
                + "\nsize " + size
                ;
        }
    }

    private enum Operator {
            STRING_EXPR,
            EXISTS_EXPR,
            MISSING_EXPR,
        AND,
        OR,
        NOT,
        EQ,
        NE,
        LT,
        LTE,
        GT,
        GTE
    }

    public static class Expression {

        public Operator op;
        public List<Object> operands;

        Expression(Operator op) {
            this.op = op;
            this.operands = new ArrayList<Object>();
        }

        public String toString() {
            return "<" + op.toString() + " " + operands + ">";
        }

        /*
         * Return a query in Lucene syntax
         */
        public String toQueryString() {
            if (this.op == Operator.STRING_EXPR)
                return this.operands.get(0).toString();

            if (this.op == Operator.NOT) {
                Expression expr = (Expression) this.operands.get(0);
                return "NOT " + expr.toQueryString();
            }

            if (BOOLEANS.contains(this.op))
                return join(this.op.toString());

            if (op == Operator.EQ) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return nv.toString();
            }

            if (op == Operator.NE) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return "NOT " + nv.toString();
            }

            if (op == Operator.LT) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return nv.name + ":{* TO " + nv.value + "}";
            }

            if (op == Operator.LTE) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return nv.name + ":[* TO " + nv.value + "]";
            }

            if (op == Operator.GT) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return nv.name + ":{" + nv.value + " TO *}";
            }

            if (op == Operator.GTE) {
                Util.NameValue nv = (Util.NameValue) this.operands.get(0);
                return nv.name + ":[" + nv.value + " TO *]";
            }

            return this.toString();
        }

        public boolean isExistsExpression() {
            return op == Operator.EXISTS_EXPR;
        }

        public boolean isMissingExpression() {
            return op == Operator.MISSING_EXPR;
        }

        public String join(String sep) {
            Expression expr = (Expression) this.operands.get(0);

            StringBuilder sb = new StringBuilder();
            sb.append(expr.toQueryString());

            for (int i=1; i < this.operands.size(); i++) {
                expr = (Expression) this.operands.get(i);
                sb.append(' ').append(sep).append(' ').append(expr.toQueryString());
            }

            return sb.toString();
        }

        public Expression addOperand(Object expr) {
            this.operands.add(expr);
            return this;
        }

        public Object getOperand() {
            return this.operands.get(0);
        }

        static Expression singleOperand(Operator op, Object expr) {
            return new Expression(op).addOperand(expr);
        }

        static Expression nameValueExpression(Operator op, String name, Object value) {
            return new Expression(op).addOperand(new Util.NameValue(name, value));
        }
    }

        /*
         * Parse exception
         */
    public static class ParseException extends Exception {
        ParseException(String message) {
            super(message);
        }
    }

    private static final Map<String, Keyword> KEYWORDS = new HashMap<String, Keyword>();
    static {
        for (Keyword k : Keyword.values())
            KEYWORDS.put(k.toString(), k);
    }

    private static final Set<Keyword> ORDERKEYS = new HashSet<Keyword>();
    static {
            ORDERKEYS.add(Keyword.ASC);
            ORDERKEYS.add(Keyword.DESC);
    }

    private static final Set<Operator> BOOLEANS = new HashSet<Operator>();
    static {
            BOOLEANS.add(Operator.AND);
            BOOLEANS.add(Operator.OR);
    }

    private final String m_queryString;
    private final StreamTokenizer m_tokenizer;
    private final ElseParser.Query m_query;
    private boolean m_parsed;

    ElseParser(final String queryString)
    {
        m_queryString = queryString;
        m_parsed = false;
        m_query = new ElseParser.Query();

        m_tokenizer = new StreamTokenizer(new StringReader(queryString));
        m_tokenizer.resetSyntax();
        m_tokenizer.whitespaceChars(0x00, ' ');
        m_tokenizer.wordChars('0', '9');
        m_tokenizer.wordChars('A', 'Z');
        m_tokenizer.wordChars('a', 'z');
        m_tokenizer.wordChars(0xA0, 0xFF);
        m_tokenizer.wordChars('_', '_');
        m_tokenizer.wordChars('.', '.');
        m_tokenizer.quoteChar('\'');
        m_tokenizer.quoteChar('"');
        m_tokenizer.commentChar('#');

        m_tokenizer.eolIsSignificant(false);
        m_tokenizer.lowerCaseMode(false);
    }

    ElseParser.Query getQuery()
        throws Exception
    {
        parse();
        return m_query;
    }

    /*
     * Parse required keyword
     */
    private void parseKeyword(Keyword k) throws Exception {
        parseKeyword(k, false);
    }

    /*
     * Parse (optional) keyword
     */
    private boolean parseKeyword(Keyword k, boolean optional) throws Exception {
        int token = m_tokenizer.nextToken();

        if (token == StreamTokenizer.TT_WORD && k.toString().equalsIgnoreCase(m_tokenizer.sval)) {
            if (DEBUG)
                System.out.println("got keyword " + k);

            return true;
        }

        if (optional) {
            m_tokenizer.pushBack();
            return false;
        }

        throw parseError(k.toString());
    }

    /*
     * Parse keyword in set (or default)
     */
    private Keyword parseKeywords(Set<Keyword> kset, Keyword kdefault) throws Exception {

        int token = m_tokenizer.nextToken();
        Keyword k = null;

        if (token == StreamTokenizer.TT_WORD) {

            try {
                k = Keyword.valueOf(m_tokenizer.sval.toUpperCase());
            } catch(IllegalArgumentException e) {
            }
        }

        if (k != null && kset.contains(k))
            return k;

        m_tokenizer.pushBack();
        return kdefault;
    }

    private Operator parseOperators(Set<Operator> oset, Operator odefault) throws Exception {

        int token = m_tokenizer.nextToken();
        Operator op = null;

        if (token == StreamTokenizer.TT_WORD) {

            try {
                op = Operator.valueOf(m_tokenizer.sval.toUpperCase());
            } catch(IllegalArgumentException e) {
                ; // nothing to do
            }
        }

        if (op != null && oset.contains(op))
            return op;

        m_tokenizer.pushBack();
        return odefault;
    }

    /*
     * Parsing failed, throw a meaningful error
     */
    private ElseParser.ParseException parseError(String expected) throws Exception {

        switch(m_tokenizer.ttype)
        {
        case StreamTokenizer.TT_EOF:
        case StreamTokenizer.TT_EOL:
            return new ElseParser.ParseException("Expected " + expected + ", got EOL");

        case StreamTokenizer.TT_NUMBER:
            return new ElseParser.ParseException("Expected " + expected + ", got number " + m_tokenizer.sval);

        default:
            return new ElseParser.ParseException("Expected " + expected + ", got " + m_tokenizer.sval);
        }
    }

    /*
     * Parse ID
     */
    private String parseId() throws Exception {
        int token = m_tokenizer.nextToken();
        if (token == StreamTokenizer.TT_WORD) {
            String word = m_tokenizer.sval;

            if (DEBUG)
                System.out.println("got " + word);

            if (!KEYWORDS.containsKey(word.toUpperCase()))
                    return word;
        }

        m_tokenizer.pushBack();
        return null;
    }

    /*
     * Parse IDENTIFIER ( id.id... )
     */
    private String parseIdentifier() throws Exception {
            return parseIdentifier(false).name;
    }

    /*
     * Parse IDENTIFIER ( id.id... ) with optional sort order
     */
    private Util.NameValue parseIdentifier(boolean sortorder) throws Exception {
        StringBuilder sb = new StringBuilder();
        Keyword order = null;
        int state = 0; // 0: id, 1: sep, 2: sort

        for (;;) {
            //
            // expecting ID
            //
            if (state == 0) {
                    String word = parseId();
                if (word != null) {
                    sb.append(word);
                    state = 1;
                }
            }

            //
            // expecting SEPARATOR
            //
            if (state == 1) {
                if (parseToken(ID_SEPARATOR, true)) {
                    sb.append(ID_SEPARATOR);
                    state = 0;
                    continue;
                }

                if (sortorder)
                    state = 2;
            }

            //
            // expect sortorder
            //
            if (state == 2)
                order = parseKeywords(ORDERKEYS, Keyword.ASC);

            break;
        }

        if (sb.length() > 0)
            return new Util.NameValue(sb.toString(), order==null ? null : order.toString().toLowerCase());

        throw parseError("identifier");
    }

    /*
     * Parse (comma separated) list of IDENTIFIERS
     */
    private List<String> parseIdentifiers() throws Exception {

        List<String> result = new ArrayList<String>();

        for (;;) {
            result.add(parseIdentifier());

            if (parseToken(LIST_SEPARATOR, true)==false)
                break;
        }

        return result;
    }

    /*
     * Parse (comma separated) list of IDENTIFIERS (for sort/order by)
     */
    private List<Util.NameValue> parseOrderIdentifiers() throws Exception {

        List<Util.NameValue> result = new ArrayList<Util.NameValue>();

        for (;;) {
            result.add(parseIdentifier(true));

            if (parseToken(LIST_SEPARATOR, true)==false)
                break;
        }

        return result;
    }

    /*
     * Parse (optional) TOKEN
     */
    private boolean parseToken(char tokenChar, boolean optional) throws Exception {
        int token = m_tokenizer.nextToken();

        if (token == tokenChar)
            return true;

        if (optional) {
            m_tokenizer.pushBack();
            return false;
        }

        throw parseError("\"" + tokenChar + '"');
    }

    /*
     * Parse NUMBER
     */
    private int parseInteger() throws Exception {
        int token = m_tokenizer.nextToken();

        if (token == StreamTokenizer.TT_NUMBER)
            return (int) m_tokenizer.nval;

        else if (token == StreamTokenizer.TT_WORD)
            try {
                return Integer.parseInt(m_tokenizer.sval);
            } catch(NumberFormatException e) {
                ; // follow through
            }

        throw parseError("integer");
    }

    /*
     * Parse (quoted) string
     */
    private String parseString() throws Exception {
        int token = m_tokenizer.nextToken();
        if (token == '"' || token == '\'')
            return m_tokenizer.sval;

        throw parseError("quoted string");
    }

    private String parseOptionalString() throws Exception {
        int token = m_tokenizer.nextToken();
        if (token == '"' || token == '\'')
            return m_tokenizer.sval;

        m_tokenizer.pushBack();
        return null;
    }

    /*
     * Parse value (string or number)
     */
    private Object parseValue() throws Exception {
        int token = m_tokenizer.nextToken();
        Object value = null;

        if (token == '"' || token == '\'')
            value = m_tokenizer.sval;

        else if (token == StreamTokenizer.TT_NUMBER)
            value =  new Double(m_tokenizer.nval);

        else if (token == StreamTokenizer.TT_WORD && Character.isDigit(m_tokenizer.sval.charAt(0)))
            value = m_tokenizer.sval;

        else
            throw parseError("value");

        if (DEBUG)
            System.out.println("got value " + value + "/s:" + m_tokenizer.sval + "/n:" + m_tokenizer.nval);

        return value;
    }

    private Operator parseOperator() throws Exception {
        int token = m_tokenizer.nextToken();
        Operator op = null;

        switch(token)
        {
        case '=':
            op = Operator.EQ;
            break;

        case '!':
            token = m_tokenizer.nextToken();
            if (token == '=')
                op = Operator.NE;
            else
                throw parseError("=");
            break;

        case '>':
            token = m_tokenizer.nextToken();
            if (token == '=')
                op = Operator.GTE;
            else {
                m_tokenizer.pushBack();
                op = Operator.GT;
            }
            break;

        case '<':
            token = m_tokenizer.nextToken();
            if (token == '=')
                op = Operator.LTE;
            else {
                m_tokenizer.pushBack();
                op = Operator.LT;
            }
            break;

        default:
            throw parseError("operator");
        }

        if (DEBUG)
            System.out.println("got operator " + op);

        return op;
    }

    private Expression addExpression(Expression result, Expression current) {
        if (result == null)
            return current;
        
        result.addOperand(current);
        return result;
    }

    private Expression addExpression(Expression result, Operator op, Expression current) {

        if (result == null)
            return Expression.singleOperand(op, current);

        result.addOperand(current);
        if (result.op == op)
            return result;

        return Expression.singleOperand(op, result);
    }

    private boolean parseDone() throws Exception {

        int token = m_tokenizer.nextToken();

        if (DEBUG)
            System.out.println("got " + token  + "/s:" + m_tokenizer.sval + "/n:" + m_tokenizer.nval);

        if  (token == StreamTokenizer.TT_EOL || token == StreamTokenizer.TT_EOF)
            return true;

        m_tokenizer.pushBack();
        return false;
    }

    private void parseEnd() throws Exception {
        int token = m_tokenizer.nextToken();

        if (DEBUG)
            System.out.println("got " + token  + "/s:" + m_tokenizer.sval + "/n:" + m_tokenizer.nval);

        if  (token == StreamTokenizer.TT_EOL || token == StreamTokenizer.TT_EOF)
            return;

        throw parseError("EOL");
    }

    private Expression parseExpression() throws Exception {

        Expression result = null;

        while (! parseDone()) {
            boolean not = parseKeyword(Keyword.NOT, true);
            Expression expr;

            String stringExpr = parseOptionalString();
            if (stringExpr != null) {
                expr = Expression.singleOperand(Operator.STRING_EXPR, stringExpr);
            } else {
                String name = parseIdentifier();
                Operator op = parseOperator();
                Object value = parseValue();

                expr = Expression.nameValueExpression(op, name, value);
            }

            if (not)
                expr = Expression.singleOperand(Operator.NOT, expr);

            Operator obool = parseOperators(BOOLEANS, null);
            if (obool == null)
                return addExpression(result, expr);

            result = addExpression(result, obool, expr);
        }

        return result;
    }

    private Expression parseFilter() throws Exception {
        if (parseKeyword(Keyword.EXIST, true)) {
            String field = parseIdentifier();
            return Expression.singleOperand(Operator.EXISTS_EXPR, field);
        }

        else if (parseKeyword(Keyword.MISSING, true)) {
            String field = parseIdentifier();
            return Expression.singleOperand(Operator.MISSING_EXPR, field);
        }

        else
            return parseExpression();
    }

        /*
         * parse scriptId = "script expression"
         */
    private Util.NameValue parseScript() throws Exception {

        String id = parseId();

        Operator op = parseOperator();
        if (op != Operator.EQ)
            throw new ElseParser.ParseException("Expected '=', got " + op);

        String script = parseString();

        return new Util.NameValue(id, script);
    }

    /*
     * Parse ELSEQL statement
     */
    private void parse()
        throws Exception
    {
        if (m_parsed)
            return;

        parseKeyword(Keyword.SELECT);

        if (parseToken(ALL_FIELDS, true))
            m_query.selectList = null;  // all fields
        else
            m_query.selectList = parseIdentifiers();

        if (parseKeyword(Keyword.FACETS, true))
            m_query.facetList = parseIdentifiers();

        if (parseKeyword(Keyword.SCRIPT, true))
            m_query.script = parseScript();

        parseKeyword(Keyword.FROM);
        m_query.index = parseIdentifier();

        if (parseKeyword(Keyword.WHERE, true))
            m_query.whereExpr = parseExpression();

        if (parseKeyword(Keyword.FILTER, true))
            m_query.filterExpr = parseFilter();

        if (parseKeyword(Keyword.ORDER, true)) {
            parseKeyword(Keyword.BY);
            m_query.orderList = parseOrderIdentifiers();
        }

        if (parseKeyword(Keyword.LIMIT, true)) {
            int v = parseInteger();

            if (parseToken(LIST_SEPARATOR, true)) {
                    m_query.from = (int) v;

                v = parseInteger();
            }

            m_query.size = (int) v;
        }

        parseEnd();
    }

    public static void main(String args[]) throws Exception {

        ElseParser parser = new ElseParser(args[0]);
        System.out.println(parser.getQuery().toString());
    }
}
