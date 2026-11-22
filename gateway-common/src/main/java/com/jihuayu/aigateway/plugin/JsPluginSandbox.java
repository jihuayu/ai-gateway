package com.jihuayu.aigateway.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Zero-Trust ECMAScript/JS Plugin Compiler & Sandbox Interpreter (#41).
 *
 * <p>Designed for both JVM and Oracle GraalVM 25 Native Image modes without reflection or host escape risks:
 * <ul>
 *   <li>Strict zero-trust static & runtime blocking of host escape tokens ({@code Java.type}, {@code Packages},
 *       {@code System}, {@code ProcessBuilder}, {@code Runtime}, {@code Thread}, {@code eval}, {@code Function}).</li>
 *   <li>Deterministic statement fuel metering ({@code maxStatements}) that aborts infinite loops
 *       ({@code while(true){}}, {@code for(;;){}}) in microseconds with {@link JsSandboxFuelExhaustedException}.</li>
 *   <li>Full support for all 5 gateway lifecycle hook functions ({@code onRequestReceived}, {@code onPreAdmit},
 *       {@code onPostAdmitPreDispatch}, {@code onPreUpstream}, {@code onResponseContent}, {@code onPostSettle}).</li>
 * </ul>
 */
public class JsPluginSandbox {

    public static class JsSandboxSecurityException extends RuntimeException {
        public JsSandboxSecurityException(String message) {
            super(message);
        }
    }

    public static class JsSandboxFuelExhaustedException extends RuntimeException {
        private final long executedStatements;

        public JsSandboxFuelExhaustedException(long executedStatements, long maxStatements) {
            super("JS plugin exceeded statement fuel limit (" + executedStatements + " > " + maxStatements + ")");
            this.executedStatements = executedStatements;
        }

        public long executedStatements() {
            return executedStatements;
        }
    }

    private static final Set<String> FORBIDDEN_IDENTIFIERS = Set.of(
        "Java", "Packages", "System", "ProcessBuilder", "Runtime", "Thread",
        "Class", "ClassLoader", "eval", "Function", "require", "process", "globalThis"
    );

    private static final Pattern FORBIDDEN_PATTERN = Pattern.compile(
        "\\b(Java\\s*\\.\\s*type|Packages|java\\s*\\.\\s*lang|ProcessBuilder|Runtime\\s*\\.\\s*getRuntime|System\\s*\\.\\s*(exit|getenv|getProperty)|eval\\s*\\(|new\\s+Function\\s*\\()"
    );

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record ExecutionOutcome(
        PluginResult result,
        long executedStatements
    ) {}

    public static final class CompiledJsPlugin implements GatewayPlugin {
        private final String id;
        private final String name;
        private final String codeSha256;
        private final String sourceCode;
        private final int priority;
        private final int maxStatements;
        private final Map<String, FuncDecl> functions;

        private CompiledJsPlugin(
            String id,
            String name,
            String codeSha256,
            String sourceCode,
            int priority,
            int maxStatements,
            Map<String, FuncDecl> functions
        ) {
            this.id = id;
            this.name = name;
            this.codeSha256 = codeSha256;
            this.sourceCode = sourceCode;
            this.priority = priority;
            this.maxStatements = maxStatements > 0 ? maxStatements : 10_000;
            this.functions = functions;
        }

        @Override
        public String id() { return id; }

        @Override
        public String name() { return name; }

        @Override
        public String type() { return "JS"; }

        @Override
        public int priority() { return priority; }

        public String codeSha256() { return codeSha256; }
        public String sourceCode() { return sourceCode; }
        public int maxStatements() { return maxStatements; }
        public boolean hasHook(String hookName) { return functions.containsKey(hookName); }

        public ExecutionOutcome executeWithMetrics(PluginStage stage, PluginExecutionContext ctx) {
            String hookName = switch (stage) {
                case AUTHENTICATE -> "onAuthenticate";
                case REQUEST_RECEIVED -> "onRequestReceived";
                case RATE_LIMIT -> "onRateLimit";
                case PRE_ADMIT -> "onPreAdmit";
                case POST_ADMIT_PRE_DISPATCH -> "onPostAdmitPreDispatch";
                case ROUTE_SELECT -> "onRouteSelect";
                case PRE_UPSTREAM -> "onPreUpstream";
                case STREAM_CHUNK -> "onStreamChunk";
                case RESPONSE_CONTENT -> "onResponseContent";
                case POST_SETTLE -> "onPostSettle";
            };
            FuncDecl fn = functions.get(hookName);
            if (fn == null) {
                return new ExecutionOutcome(PluginResult.Continue.INSTANCE, 0L);
            }
            EvalState state = new EvalState(ctx, functions, maxStatements);
            Map<String, Object> localEnv = new LinkedHashMap<>();
            if (!fn.params().isEmpty()) {
                localEnv.put(fn.params().get(0), new JsCtxBridge(ctx));
            }
            Object ret = state.execBlock(fn.body(), localEnv);
            PluginResult res = state.toPluginResult(ret);
            return new ExecutionOutcome(res, state.executedStatements);
        }

        @Override
        public PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
            return executeWithMetrics(stage, ctx).result();
        }

        @Override
        public PluginResult onRequestReceived(PluginExecutionContext ctx) {
            return executeStage(PluginStage.REQUEST_RECEIVED, ctx);
        }

        @Override
        public PluginResult onPreAdmit(PluginExecutionContext ctx) {
            return executeStage(PluginStage.PRE_ADMIT, ctx);
        }

        @Override
        public PluginResult onPostAdmitPreDispatch(PluginExecutionContext ctx) {
            return executeStage(PluginStage.POST_ADMIT_PRE_DISPATCH, ctx);
        }

        @Override
        public PluginResult onPreUpstream(PluginExecutionContext ctx) {
            return executeStage(PluginStage.PRE_UPSTREAM, ctx);
        }

        @Override
        public PluginResult onResponseContent(PluginExecutionContext ctx) {
            return executeStage(PluginStage.RESPONSE_CONTENT, ctx);
        }

        @Override
        public void onPostSettle(PluginExecutionContext ctx) {
            executeStage(PluginStage.POST_SETTLE, ctx);
        }
    }

    /**
     * Validates and compiles JavaScript plugin source code into an immutable {@link CompiledJsPlugin}.
     */
    public static CompiledJsPlugin compile(
        String id,
        String name,
        String codeSha256,
        String sourceCode,
        int priority,
        int maxStatements
    ) {
        if (sourceCode == null || sourceCode.isBlank()) {
            throw new IllegalArgumentException("JS plugin source code cannot be blank");
        }
        String stripped = stripComments(sourceCode);
        Matcher m = FORBIDDEN_PATTERN.matcher(stripped);
        if (m.find()) {
            throw new JsSandboxSecurityException("Forbidden host access in JS plugin: " + m.group(1));
        }
        List<Token> tokens = tokenize(stripped);
        for (Token t : tokens) {
            if (t.type == TokType.IDENT && FORBIDDEN_IDENTIFIERS.contains(t.text)) {
                throw new JsSandboxSecurityException("Forbidden host identifier in JS plugin: " + t.text);
            }
        }
        Parser parser = new Parser(tokens);
        Map<String, FuncDecl> funcs = parser.parseProgram();
        return new CompiledJsPlugin(
            id != null ? id : "js-plugin",
            name != null ? name : (id != null ? id : "js-plugin"),
            codeSha256 != null ? codeSha256 : "",
            sourceCode,
            priority,
            maxStatements,
            funcs
        );
    }

    private static String stripComments(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '"' || c == '\'' || c == '`') {
                char quote = c;
                out.append(c);
                i++;
                while (i < n) {
                    char sc = src.charAt(i);
                    out.append(sc);
                    i++;
                    if (sc == '\\' && i < n) {
                        out.append(src.charAt(i));
                        i++;
                    } else if (sc == quote) {
                        break;
                    }
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                i += 2;
                while (i < n && src.charAt(i) != '\n') i++;
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < n && !(src.charAt(i) == '*' && src.charAt(i + 1) == '/')) i++;
                i += 2;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    // ---- Tokenizer & AST ----

    private enum TokType {
        IDENT, NUMBER, STRING, PUNCT, OP, EOF
    }

    private record Token(TokType type, String text) {}

    private static List<Token> tokenize(String s) {
        List<Token> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '"' || c == '\'' || c == '`') {
                char q = c;
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < n && s.charAt(i) != q) {
                    char ch = s.charAt(i++);
                    if (ch == '\\' && i < n) {
                        char esc = s.charAt(i++);
                        switch (esc) {
                            case 'n' -> sb.append('\n');
                            case 'r' -> sb.append('\r');
                            case 't' -> sb.append('\t');
                            case '"' -> sb.append('"');
                            case '\'' -> sb.append('\'');
                            case '\\' -> sb.append('\\');
                            default -> sb.append(esc);
                        }
                    } else {
                        sb.append(ch);
                    }
                }
                if (i < n) i++;
                out.add(new Token(TokType.STRING, sb.toString()));
                continue;
            }
            if (Character.isDigit(c)) {
                int start = i;
                while (i < n && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) i++;
                out.add(new Token(TokType.NUMBER, s.substring(start, i)));
                continue;
            }
            if (Character.isJavaIdentifierStart(c)) {
                int start = i;
                while (i < n && Character.isJavaIdentifierPart(s.charAt(i))) i++;
                out.add(new Token(TokType.IDENT, s.substring(start, i)));
                continue;
            }
            if (i + 2 < n) {
                String t3 = s.substring(i, i + 3);
                if (t3.equals("===") || t3.equals("!==")) {
                    out.add(new Token(TokType.OP, t3));
                    i += 3;
                    continue;
                }
            }
            if (i + 1 < n) {
                String t2 = s.substring(i, i + 2);
                if (Set.of("==", "!=", "<=", ">=", "&&", "||", "+=", "-=", "++", "--", "=>").contains(t2)) {
                    out.add(new Token(TokType.OP, t2));
                    i += 2;
                    continue;
                }
            }
            if ("+-*/%<>!=?:".indexOf(c) >= 0) {
                out.add(new Token(TokType.OP, String.valueOf(c)));
                i++;
                continue;
            }
            if ("(){}[].,;".indexOf(c) >= 0) {
                out.add(new Token(TokType.PUNCT, String.valueOf(c)));
                i++;
                continue;
            }
            throw new IllegalArgumentException("Unexpected character in JS plugin: '" + c + "'");
        }
        out.add(new Token(TokType.EOF, ""));
        return out;
    }

    private record FuncDecl(String name, List<String> params, List<Stmt> body) {}

    private sealed interface Stmt permits
        Stmt.VarStmt, Stmt.AssignStmt, Stmt.IfStmt, Stmt.WhileStmt, Stmt.ForStmt,
        Stmt.ReturnStmt, Stmt.ExprStmt, Stmt.BlockStmt {
        record VarStmt(String name, Expr init) implements Stmt {}
        record AssignStmt(Expr target, String op, Expr value) implements Stmt {}
        record IfStmt(Expr cond, List<Stmt> thenBranch, List<Stmt> elseBranch) implements Stmt {}
        record WhileStmt(Expr cond, List<Stmt> body) implements Stmt {}
        record ForStmt(Stmt init, Expr cond, Stmt update, List<Stmt> body) implements Stmt {}
        record ReturnStmt(Expr value) implements Stmt {}
        record ExprStmt(Expr expr) implements Stmt {}
        record BlockStmt(List<Stmt> stmts) implements Stmt {}
    }

    private sealed interface Expr permits
        Expr.Literal, Expr.Ident, Expr.Unary, Expr.Binary, Expr.Ternary,
        Expr.PropAccess, Expr.IndexAccess, Expr.Call, Expr.MethodCall,
        Expr.ObjLiteral, Expr.ArrLiteral {
        record Literal(Object value) implements Expr {}
        record Ident(String name) implements Expr {}
        record Unary(String op, Expr expr) implements Expr {}
        record Binary(String op, Expr left, Expr right) implements Expr {}
        record Ternary(Expr cond, Expr ifTrue, Expr ifFalse) implements Expr {}
        record PropAccess(Expr target, String prop) implements Expr {}
        record IndexAccess(Expr target, Expr index) implements Expr {}
        record Call(String funcName, List<Expr> args) implements Expr {}
        record MethodCall(Expr target, String method, List<Expr> args) implements Expr {}
        record ObjLiteral(Map<String, Expr> props) implements Expr {}
        record ArrLiteral(List<Expr> items) implements Expr {}
    }

    private static final class Parser {
        private final List<Token> tokens;
        private int pos = 0;

        Parser(List<Token> tokens) {
            this.tokens = tokens;
        }

        private Token peek() { return tokens.get(pos); }
        private Token next() { return tokens.get(pos++); }
        private boolean match(TokType type, String text) {
            Token t = peek();
            if (t.type == type && (text == null || t.text.equals(text))) {
                pos++;
                return true;
            }
            return false;
        }
        private Token expect(TokType type, String text) {
            Token t = next();
            if (t.type != type || (text != null && !t.text.equals(text))) {
                throw new IllegalArgumentException("Expected '" + text + "' but got '" + t.text + "'");
            }
            return t;
        }

        Map<String, FuncDecl> parseProgram() {
            Map<String, FuncDecl> funcs = new LinkedHashMap<>();
            while (peek().type != TokType.EOF) {
                if (match(TokType.IDENT, "export")) {
                    match(TokType.IDENT, "default");
                }
                if (match(TokType.IDENT, "async")) {
                    // allow async function keyword
                }
                if (match(TokType.IDENT, "function")) {
                    FuncDecl fn = parseFuncDecl();
                    funcs.put(fn.name(), fn);
                } else {
                    // top-level const/let/var or statement
                    Stmt st = parseStmt();
                    if (st instanceof Stmt.VarStmt vs && vs.init() instanceof Expr.ObjLiteral obj) {
                        // Ignore top-level metadata object
                    }
                }
            }
            return funcs;
        }

        private FuncDecl parseFuncDecl() {
            String name = expect(TokType.IDENT, null).text;
            expect(TokType.PUNCT, "(");
            List<String> params = new ArrayList<>();
            while (!match(TokType.PUNCT, ")")) {
                params.add(expect(TokType.IDENT, null).text);
                match(TokType.PUNCT, ",");
            }
            List<Stmt> body = parseBlock();
            return new FuncDecl(name, params, body);
        }

        private List<Stmt> parseBlock() {
            expect(TokType.PUNCT, "{");
            List<Stmt> stmts = new ArrayList<>();
            while (!match(TokType.PUNCT, "}")) {
                if (peek().type == TokType.EOF) {
                    throw new IllegalArgumentException("Unclosed block '{' in JS plugin");
                }
                stmts.add(parseStmt());
            }
            return stmts;
        }

        private Stmt parseStmt() {
            while (match(TokType.PUNCT, ";")) {}
            Token t = peek();
            if (t.type == TokType.PUNCT && t.text.equals("{")) {
                return new Stmt.BlockStmt(parseBlock());
            }
            if (t.type == TokType.IDENT) {
                switch (t.text) {
                    case "const", "let", "var" -> {
                        next();
                        String varName = expect(TokType.IDENT, null).text;
                        Expr init = null;
                        if (match(TokType.OP, "=")) {
                            init = parseExpr();
                        }
                        match(TokType.PUNCT, ";");
                        return new Stmt.VarStmt(varName, init);
                    }
                    case "if" -> {
                        next();
                        expect(TokType.PUNCT, "(");
                        Expr cond = parseExpr();
                        expect(TokType.PUNCT, ")");
                        List<Stmt> thenBranch = peek().text.equals("{") ? parseBlock() : List.of(parseStmt());
                        List<Stmt> elseBranch = List.of();
                        if (match(TokType.IDENT, "else")) {
                            elseBranch = peek().text.equals("{") ? parseBlock() : List.of(parseStmt());
                        }
                        return new Stmt.IfStmt(cond, thenBranch, elseBranch);
                    }
                    case "while" -> {
                        next();
                        expect(TokType.PUNCT, "(");
                        Expr cond = parseExpr();
                        expect(TokType.PUNCT, ")");
                        List<Stmt> body = peek().text.equals("{") ? parseBlock() : List.of(parseStmt());
                        return new Stmt.WhileStmt(cond, body);
                    }
                    case "for" -> {
                        next();
                        expect(TokType.PUNCT, "(");
                        Stmt init = null;
                        if (!match(TokType.PUNCT, ";")) {
                            init = parseStmt();
                        }
                        Expr cond = new Expr.Literal(Boolean.TRUE);
                        if (!match(TokType.PUNCT, ";")) {
                            cond = parseExpr();
                            expect(TokType.PUNCT, ";");
                        }
                        Stmt update = null;
                        if (!match(TokType.PUNCT, ")")) {
                            update = parseForUpdate();
                            expect(TokType.PUNCT, ")");
                        }
                        List<Stmt> body = peek().text.equals("{") ? parseBlock() : List.of(parseStmt());
                        return new Stmt.ForStmt(init, cond, update, body);
                    }
                    case "return" -> {
                        next();
                        if (match(TokType.PUNCT, ";") || peek().text.equals("}")) {
                            return new Stmt.ReturnStmt(new Expr.Literal(null));
                        }
                        Expr val = parseExpr();
                        match(TokType.PUNCT, ";");
                        return new Stmt.ReturnStmt(val);
                    }
                }
            }
            Stmt stmt = parseAssignOrExprStmt();
            match(TokType.PUNCT, ";");
            return stmt;
        }

        private Stmt parseForUpdate() {
            return parseAssignOrExprStmt();
        }

        private Stmt parseAssignOrExprStmt() {
            Expr expr = parseExpr();
            Token t = peek();
            if (t.type == TokType.OP && (t.text.equals("=") || t.text.equals("+=") || t.text.equals("-="))) {
                String op = next().text;
                Expr val = parseExpr();
                return new Stmt.AssignStmt(expr, op, val);
            }
            if (t.type == TokType.OP && (t.text.equals("++") || t.text.equals("--"))) {
                String op = next().text;
                return new Stmt.AssignStmt(expr, op.equals("++") ? "+=" : "-=", new Expr.Literal(1L));
            }
            return new Stmt.ExprStmt(expr);
        }

        private Expr parseExpr() {
            Expr cond = parseLogicalOr();
            if (match(TokType.OP, "?")) {
                Expr ifTrue = parseExpr();
                expect(TokType.OP, ":");
                Expr ifFalse = parseExpr();
                return new Expr.Ternary(cond, ifTrue, ifFalse);
            }
            return cond;
        }

        private Expr parseLogicalOr() {
            Expr left = parseLogicalAnd();
            while (match(TokType.OP, "||")) {
                left = new Expr.Binary("||", left, parseLogicalAnd());
            }
            return left;
        }

        private Expr parseLogicalAnd() {
            Expr left = parseEquality();
            while (match(TokType.OP, "&&")) {
                left = new Expr.Binary("&&", left, parseEquality());
            }
            return left;
        }

        private Expr parseEquality() {
            Expr left = parseComparison();
            while (true) {
                Token t = peek();
                if (t.type == TokType.OP && Set.of("===", "!==", "==", "!=").contains(t.text)) {
                    String op = next().text;
                    left = new Expr.Binary(op, left, parseComparison());
                } else {
                    break;
                }
            }
            return left;
        }

        private Expr parseComparison() {
            Expr left = parseAdditive();
            while (true) {
                Token t = peek();
                if (t.type == TokType.OP && Set.of("<", "<=", ">", ">=").contains(t.text)) {
                    String op = next().text;
                    left = new Expr.Binary(op, left, parseAdditive());
                } else {
                    break;
                }
            }
            return left;
        }

        private Expr parseAdditive() {
            Expr left = parseMultiplicative();
            while (true) {
                Token t = peek();
                if (t.type == TokType.OP && (t.text.equals("+") || t.text.equals("-"))) {
                    String op = next().text;
                    left = new Expr.Binary(op, left, parseMultiplicative());
                } else {
                    break;
                }
            }
            return left;
        }

        private Expr parseMultiplicative() {
            Expr left = parseUnary();
            while (true) {
                Token t = peek();
                if (t.type == TokType.OP && Set.of("*", "/", "%").contains(t.text)) {
                    String op = next().text;
                    left = new Expr.Binary(op, left, parseUnary());
                } else {
                    break;
                }
            }
            return left;
        }

        private Expr parseUnary() {
            Token t = peek();
            if (t.type == TokType.OP && (t.text.equals("!") || t.text.equals("-"))) {
                String op = next().text;
                return new Expr.Unary(op, parseUnary());
            }
            if (t.type == TokType.IDENT && t.text.equals("typeof")) {
                next();
                return new Expr.Unary("typeof", parseUnary());
            }
            return parsePostfix();
        }

        private Expr parsePostfix() {
            Expr expr = parsePrimary();
            while (true) {
                if (match(TokType.PUNCT, ".")) {
                    String prop = expect(TokType.IDENT, null).text;
                    if (match(TokType.PUNCT, "(")) {
                        List<Expr> args = parseArgList();
                        expr = new Expr.MethodCall(expr, prop, args);
                    } else {
                        expr = new Expr.PropAccess(expr, prop);
                    }
                } else if (match(TokType.PUNCT, "[")) {
                    Expr idx = parseExpr();
                    expect(TokType.PUNCT, "]");
                    expr = new Expr.IndexAccess(expr, idx);
                } else if (expr instanceof Expr.Ident id && match(TokType.PUNCT, "(")) {
                    List<Expr> args = parseArgList();
                    expr = new Expr.Call(id.name(), args);
                } else {
                    break;
                }
            }
            return expr;
        }

        private List<Expr> parseArgList() {
            List<Expr> args = new ArrayList<>();
            while (!match(TokType.PUNCT, ")")) {
                args.add(parseExpr());
                match(TokType.PUNCT, ",");
            }
            return args;
        }

        private Expr parsePrimary() {
            Token t = next();
            return switch (t.type) {
                case NUMBER -> {
                    if (t.text.contains(".")) {
                        yield new Expr.Literal(Double.parseDouble(t.text));
                    }
                    yield new Expr.Literal(Long.parseLong(t.text));
                }
                case STRING -> new Expr.Literal(t.text);
                case IDENT -> switch (t.text) {
                    case "true" -> new Expr.Literal(Boolean.TRUE);
                    case "false" -> new Expr.Literal(Boolean.FALSE);
                    case "null", "undefined" -> new Expr.Literal(null);
                    default -> new Expr.Ident(t.text);
                };
                case PUNCT -> {
                    if (t.text.equals("(")) {
                        Expr inner = parseExpr();
                        expect(TokType.PUNCT, ")");
                        yield inner;
                    }
                    if (t.text.equals("{")) {
                        Map<String, Expr> props = new LinkedHashMap<>();
                        while (!match(TokType.PUNCT, "}")) {
                            Token kt = next();
                            String key = kt.text;
                            expect(TokType.OP, ":");
                            Expr val = parseExpr();
                            props.put(key, val);
                            match(TokType.PUNCT, ",");
                        }
                        yield new Expr.ObjLiteral(props);
                    }
                    if (t.text.equals("[")) {
                        List<Expr> items = new ArrayList<>();
                        while (!match(TokType.PUNCT, "]")) {
                            items.add(parseExpr());
                            match(TokType.PUNCT, ",");
                        }
                        yield new Expr.ArrLiteral(items);
                    }
                    throw new IllegalArgumentException("Unexpected token '" + t.text + "' in JS plugin");
                }
                default -> throw new IllegalArgumentException("Unexpected token '" + t.text + "' in JS plugin");
            };
        }
    }

    // ---- Runtime Evaluator ----

    private record ReturnSignal(Object value) {}

    public static final class JsCtxBridge {
        private final PluginExecutionContext ctx;

        public JsCtxBridge(PluginExecutionContext ctx) {
            this.ctx = ctx;
        }

        public PluginExecutionContext raw() { return ctx; }
    }

    private static final class EvalState {
        private final PluginExecutionContext ctx;
        private final Map<String, FuncDecl> functions;
        private final long maxStatements;
        private long executedStatements = 0L;

        EvalState(PluginExecutionContext ctx, Map<String, FuncDecl> functions, long maxStatements) {
            this.ctx = ctx;
            this.functions = functions;
            this.maxStatements = maxStatements;
        }

        private void consumeFuel() {
            executedStatements++;
            if (executedStatements > maxStatements) {
                throw new JsSandboxFuelExhaustedException(executedStatements, maxStatements);
            }
        }

        Object execBlock(List<Stmt> stmts, Map<String, Object> env) {
            for (Stmt st : stmts) {
                Object res = execStmt(st, env);
                if (res instanceof ReturnSignal sig) {
                    return sig.value();
                }
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        private Object execStmt(Stmt st, Map<String, Object> env) {
            consumeFuel();
            return switch (st) {
                case Stmt.BlockStmt bs -> {
                    for (Stmt inner : bs.stmts()) {
                        Object r = execStmt(inner, env);
                        if (r instanceof ReturnSignal) yield r;
                    }
                    yield null;
                }
                case Stmt.VarStmt vs -> {
                    Object val = vs.init() != null ? evalExpr(vs.init(), env) : null;
                    env.put(vs.name(), val);
                    yield null;
                }
                case Stmt.AssignStmt as -> {
                    Object rhs = evalExpr(as.value(), env);
                    applyAssign(as.target(), as.op(), rhs, env);
                    yield null;
                }
                case Stmt.IfStmt is -> {
                    if (isTruthy(evalExpr(is.cond(), env))) {
                        for (Stmt inner : is.thenBranch()) {
                            Object r = execStmt(inner, env);
                            if (r instanceof ReturnSignal) yield r;
                        }
                    } else {
                        for (Stmt inner : is.elseBranch()) {
                            Object r = execStmt(inner, env);
                            if (r instanceof ReturnSignal) yield r;
                        }
                    }
                    yield null;
                }
                case Stmt.WhileStmt ws -> {
                    while (isTruthy(evalExpr(ws.cond(), env))) {
                        consumeFuel();
                        for (Stmt inner : ws.body()) {
                            Object r = execStmt(inner, env);
                            if (r instanceof ReturnSignal) yield r;
                        }
                    }
                    yield null;
                }
                case Stmt.ForStmt fs -> {
                    if (fs.init() != null) execStmt(fs.init(), env);
                    while (isTruthy(evalExpr(fs.cond(), env))) {
                        consumeFuel();
                        for (Stmt inner : fs.body()) {
                            Object r = execStmt(inner, env);
                            if (r instanceof ReturnSignal) yield r;
                        }
                        if (fs.update() != null) execStmt(fs.update(), env);
                    }
                    yield null;
                }
                case Stmt.ReturnStmt rs -> new ReturnSignal(evalExpr(rs.value(), env));
                case Stmt.ExprStmt es -> {
                    evalExpr(es.expr(), env);
                    yield null;
                }
            };
        }

        @SuppressWarnings("unchecked")
        private void applyAssign(Expr target, String op, Object rhs, Map<String, Object> env) {
            if (target instanceof Expr.Ident id) {
                Object cur = env.get(id.name());
                Object nextVal = computeAssignValue(cur, op, rhs);
                env.put(id.name(), nextVal);
            } else if (target instanceof Expr.PropAccess pa) {
                Object obj = evalExpr(pa.target(), env);
                if (obj instanceof JsCtxBridge bridge) {
                    setCtxProp(bridge.raw(), pa.prop(), rhs);
                } else if (obj instanceof Map<?, ?> m) {
                    Map<String, Object> map = (Map<String, Object>) m;
                    Object cur = map.get(pa.prop());
                    map.put(pa.prop(), computeAssignValue(cur, op, rhs));
                    if (map == ctx.requestJson()) {
                        ctx.markRequestBodyMutated();
                    } else if (map == ctx.responseJson()) {
                        ctx.setResponseJson(map);
                    }
                }
            } else if (target instanceof Expr.IndexAccess ia) {
                Object obj = evalExpr(ia.target(), env);
                Object idx = evalExpr(ia.index(), env);
                if (obj instanceof Map<?, ?> m && idx != null) {
                    Map<String, Object> map = (Map<String, Object>) m;
                    String key = String.valueOf(idx);
                    Object cur = map.get(key);
                    map.put(key, computeAssignValue(cur, op, rhs));
                    if (map == ctx.requestJson()) {
                        ctx.markRequestBodyMutated();
                    } else if (map == ctx.responseJson()) {
                        ctx.setResponseJson(map);
                    }
                } else if (obj instanceof List<?> list && idx instanceof Number n) {
                    List<Object> l = (List<Object>) list;
                    int i = n.intValue();
                    if (i >= 0 && i < l.size()) {
                        l.set(i, computeAssignValue(l.get(i), op, rhs));
                        ctx.markRequestBodyMutated();
                    }
                }
            }
        }

        private void setCtxProp(PluginExecutionContext c, String prop, Object val) {
            switch (prop) {
                case "model", "modelAlias" -> c.setModelAlias(val != null ? String.valueOf(val) : "");
                case "responseBody" -> c.setResponseBody(val != null ? String.valueOf(val) : "");
                case "responseStatus" -> {
                    if (val instanceof Number n) c.setResponseStatus(n.intValue());
                }
                case "streamChunk" -> c.setStreamChunk(val != null ? String.valueOf(val) : "");
                case "endUserId" -> c.setEndUserId(val != null ? String.valueOf(val) : null);
                default -> c.attributes().put(prop, val);
            }
        }

        private Object computeAssignValue(Object cur, String op, Object rhs) {
            return switch (op) {
                case "=" -> rhs;
                case "+=" -> evalBinary("+", cur, rhs);
                case "-=" -> evalBinary("-", cur, rhs);
                default -> rhs;
            };
        }

        @SuppressWarnings("unchecked")
        private Object evalExpr(Expr expr, Map<String, Object> env) {
            return switch (expr) {
                case Expr.Literal lit -> lit.value();
                case Expr.Ident id -> {
                    if (env.containsKey(id.name())) yield env.get(id.name());
                    if (Set.of("PluginResult", "JSON", "Math", "Crypto", "Base64", "RegExp").contains(id.name())) {
                        yield id.name();
                    }
                    yield null;
                }
                case Expr.Unary u -> {
                    Object v = evalExpr(u.expr(), env);
                    yield switch (u.op()) {
                        case "!" -> !isTruthy(v);
                        case "-" -> (v instanceof Number n) ? (n instanceof Double ? -n.doubleValue() : -n.longValue()) : 0L;
                        case "typeof" -> v == null ? "undefined" : (v instanceof Boolean ? "boolean" : (v instanceof Number ? "number" : (v instanceof String ? "string" : "object")));
                        default -> v;
                    };
                }
                case Expr.Binary b -> {
                    if ("&&".equals(b.op())) {
                        Object l = evalExpr(b.left(), env);
                        yield isTruthy(l) ? evalExpr(b.right(), env) : l;
                    }
                    if ("||".equals(b.op())) {
                        Object l = evalExpr(b.left(), env);
                        yield isTruthy(l) ? l : evalExpr(b.right(), env);
                    }
                    Object l = evalExpr(b.left(), env);
                    Object r = evalExpr(b.right(), env);
                    yield evalBinary(b.op(), l, r);
                }
                case Expr.Ternary t -> isTruthy(evalExpr(t.cond(), env))
                    ? evalExpr(t.ifTrue(), env)
                    : evalExpr(t.ifFalse(), env);
                case Expr.PropAccess pa -> {
                    Object target = evalExpr(pa.target(), env);
                    yield getProperty(target, pa.prop());
                }
                case Expr.IndexAccess ia -> {
                    Object target = evalExpr(ia.target(), env);
                    Object idx = evalExpr(ia.index(), env);
                    if (target instanceof Map<?, ?> m && idx != null) {
                        yield m.get(String.valueOf(idx));
                    }
                    if (target instanceof List<?> l && idx instanceof Number n) {
                        int i = n.intValue();
                        yield (i >= 0 && i < l.size()) ? l.get(i) : null;
                    }
                    if (target instanceof String s && idx instanceof Number n) {
                        int i = n.intValue();
                        yield (i >= 0 && i < s.length()) ? String.valueOf(s.charAt(i)) : null;
                    }
                    yield null;
                }
                case Expr.Call call -> {
                    List<Object> args = new ArrayList<>(call.args().size());
                    for (Expr a : call.args()) args.add(evalExpr(a, env));
                    FuncDecl userFn = functions.get(call.funcName());
                    if (userFn != null) {
                        Map<String, Object> fnEnv = new LinkedHashMap<>();
                        for (int i = 0; i < userFn.params().size() && i < args.size(); i++) {
                            fnEnv.put(userFn.params().get(i), args.get(i));
                        }
                        yield execBlock(userFn.body(), fnEnv);
                    }
                    yield null;
                }
                case Expr.MethodCall mc -> {
                    Object target = evalExpr(mc.target(), env);
                    List<Object> args = new ArrayList<>(mc.args().size());
                    for (Expr a : mc.args()) args.add(evalExpr(a, env));
                    yield invokeMethod(target, mc.method(), args);
                }
                case Expr.ObjLiteral obj -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (Map.Entry<String, Expr> e : obj.props().entrySet()) {
                        map.put(e.getKey(), evalExpr(e.getValue(), env));
                    }
                    yield map;
                }
                case Expr.ArrLiteral arr -> {
                    List<Object> list = new ArrayList<>(arr.items().size());
                    for (Expr item : arr.items()) {
                        list.add(evalExpr(item, env));
                    }
                    yield list;
                }
            };
        }

        private Object getProperty(Object target, String prop) {
            if (target == null) return null;
            if (target instanceof JsCtxBridge bridge) {
                PluginExecutionContext c = bridge.raw();
                return switch (prop) {
                    case "requestId" -> c.requestId();
                    case "projectId" -> c.projectId();
                    case "workspaceId" -> c.workspaceId();
                    case "keyId" -> c.keyId();
                    case "endpoint" -> c.endpoint();
                    case "model", "modelAlias" -> c.modelAlias();
                    case "stream" -> c.stream();
                    case "headers", "requestHeaders" -> c.requestHeaders();
                    case "body", "requestJson" -> c.requestJson();
                    case "responseHeaders" -> c.responseHeaders();
                    case "responseStatus" -> c.responseStatus();
                    case "responseBody" -> c.responseBody();
                    case "response", "responseJson" -> c.responseJson();
                    case "config" -> c.pluginConfig();
                    case "attributes" -> c.attributes();
                    case "promptText" -> c.extractPromptText();
                    case "actualMicros" -> c.settledActualMicros();
                    case "inputTokens" -> c.settledInputTokens();
                    case "outputTokens" -> c.settledOutputTokens();
                    case "kv" -> c.kv();
                    case "crypto" -> c.crypto();
                    case "queryParams" -> c.queryParams();
                    case "endUserId" -> c.endUserId();
                    case "streamChunk" -> c.streamChunk();
                    case "attributionTags" -> c.attributionTags();
                    default -> c.attributes().get(prop);
                };
            }
            if (target instanceof Map<?, ?> m) {
                return m.get(prop);
            }
            if (target instanceof List<?> l && "length".equals(prop)) {
                return (long) l.size();
            }
            if (target instanceof String s && "length".equals(prop)) {
                return (long) s.length();
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        private Object invokeMethod(Object target, String method, List<Object> args) {
            if ("PluginResult".equals(target)) {
                return switch (method) {
                    case "Continue", "continueExecution" -> PluginResult.Continue.INSTANCE;
                    case "Reject", "reject" -> {
                        int status = args.size() > 0 && args.get(0) instanceof Number n ? n.intValue() : 400;
                        String code = args.size() > 1 && args.get(1) != null ? String.valueOf(args.get(1)) : "plugin_rejected";
                        String msg = args.size() > 2 && args.get(2) != null ? String.valueOf(args.get(2)) : "Rejected by JS plugin";
                        yield PluginResult.reject(status, code, msg);
                    }
                    case "ShortCircuit", "shortCircuit" -> {
                        int status = args.size() > 0 && args.get(0) instanceof Number n ? n.intValue() : 200;
                        Object rawBody = args.size() > 1 ? args.get(1) : "{}";
                        String bodyStr;
                        try {
                            bodyStr = (rawBody instanceof String s) ? s : MAPPER.writeValueAsString(rawBody);
                        } catch (Exception e) {
                            bodyStr = String.valueOf(rawBody);
                        }
                        Map<String, String> hdrs = new LinkedHashMap<>();
                        if (args.size() > 2 && args.get(2) instanceof Map<?, ?> m) {
                            for (Map.Entry<?, ?> e : m.entrySet()) {
                                if (e.getKey() != null && e.getValue() != null) {
                                    hdrs.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                                }
                            }
                        }
                        yield PluginResult.shortCircuit(status, bodyStr, hdrs);
                    }
                    default -> PluginResult.Continue.INSTANCE;
                };
            }
            if ("JSON".equals(target)) {
                try {
                    if ("stringify".equals(method) && !args.isEmpty()) {
                        return MAPPER.writeValueAsString(args.get(0));
                    }
                    if ("parse".equals(method) && !args.isEmpty() && args.get(0) != null) {
                        return MAPPER.readValue(String.valueOf(args.get(0)), Map.class);
                    }
                } catch (Exception e) {
                    return null;
                }
            }
            if ("Crypto".equals(target) || target instanceof PluginExecutionContext.PluginCrypto) {
                PluginExecutionContext.PluginCrypto pc = PluginExecutionContext.PluginCrypto.INSTANCE;
                return switch (method) {
                    case "sha256" -> !args.isEmpty() ? pc.sha256(String.valueOf(args.get(0))) : "";
                    case "hmacSha256" -> args.size() >= 2 ? pc.hmacSha256(String.valueOf(args.get(0)), String.valueOf(args.get(1))) : "";
                    case "base64Encode", "encode" -> !args.isEmpty() ? pc.base64Encode(String.valueOf(args.get(0))) : "";
                    case "base64Decode", "decode" -> !args.isEmpty() ? pc.base64Decode(String.valueOf(args.get(0))) : "";
                    case "constantTimeEquals" -> args.size() >= 2 && pc.constantTimeEquals(String.valueOf(args.get(0)), String.valueOf(args.get(1)));
                    default -> null;
                };
            }
            if ("Base64".equals(target)) {
                PluginExecutionContext.PluginCrypto pc = PluginExecutionContext.PluginCrypto.INSTANCE;
                return switch (method) {
                    case "encode" -> !args.isEmpty() ? pc.base64Encode(String.valueOf(args.get(0))) : "";
                    case "decode" -> !args.isEmpty() ? pc.base64Decode(String.valueOf(args.get(0))) : "";
                    case "urlDecode" -> !args.isEmpty() ? pc.base64UrlDecode(String.valueOf(args.get(0))) : "";
                    default -> null;
                };
            }
            if ("RegExp".equals(target)) {
                return switch (method) {
                    case "test" -> args.size() >= 2 && args.get(0) != null && args.get(1) != null
                        && Pattern.compile(String.valueOf(args.get(0))).matcher(String.valueOf(args.get(1))).find();
                    case "replace" -> args.size() >= 3 && args.get(0) != null && args.get(1) != null && args.get(2) != null
                        ? Pattern.compile(String.valueOf(args.get(0))).matcher(String.valueOf(args.get(1))).replaceAll(String.valueOf(args.get(2)))
                        : (args.size() >= 2 ? String.valueOf(args.get(1)) : "");
                    default -> null;
                };
            }
            if (target instanceof PluginExecutionContext.PluginKvStore kv) {
                return switch (method) {
                    case "get" -> !args.isEmpty() && args.get(0) != null ? kv.get(String.valueOf(args.get(0))) : null;
                    case "set" -> {
                        if (args.size() >= 2 && args.get(0) != null) {
                            long ttl = args.size() >= 3 && args.get(2) instanceof Number n ? n.longValue() : 300L;
                            kv.set(String.valueOf(args.get(0)), String.valueOf(args.get(1)), ttl);
                        }
                        yield null;
                    }
                    case "setNx" -> {
                        if (args.size() >= 2 && args.get(0) != null) {
                            long ttl = args.size() >= 3 && args.get(2) instanceof Number n ? n.longValue() : 300L;
                            yield kv.setNx(String.valueOf(args.get(0)), String.valueOf(args.get(1)), ttl);
                        }
                        yield false;
                    }
                    case "incr" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            long delta = args.size() >= 2 && args.get(1) instanceof Number n ? n.longValue() : 1L;
                            long ttl = args.size() >= 3 && args.get(2) instanceof Number n ? n.longValue() : 60L;
                            yield kv.incr(String.valueOf(args.get(0)), delta, ttl);
                        }
                        yield 0L;
                    }
                    case "del" -> {
                        if (!args.isEmpty() && args.get(0) != null) kv.del(String.valueOf(args.get(0)));
                        yield null;
                    }
                    default -> null;
                };
            }
            if ("Math".equals(target)) {
                if ("max".equals(method) && args.size() >= 2 && args.get(0) instanceof Number a && args.get(1) instanceof Number b) {
                    return Math.max(a.doubleValue(), b.doubleValue());
                }
                if ("min".equals(method) && args.size() >= 2 && args.get(0) instanceof Number a && args.get(1) instanceof Number b) {
                    return Math.min(a.doubleValue(), b.doubleValue());
                }
                if ("floor".equals(method) && !args.isEmpty() && args.get(0) instanceof Number a) {
                    return (long) Math.floor(a.doubleValue());
                }
            }
            if (target instanceof JsCtxBridge bridge) {
                PluginExecutionContext c = bridge.raw();
                return switch (method) {
                    case "setHeader", "setRequestHeader" -> {
                        if (args.size() >= 2 && args.get(0) != null && args.get(1) != null) {
                            c.setRequestHeader(String.valueOf(args.get(0)), String.valueOf(args.get(1)));
                        }
                        yield null;
                    }
                    case "getHeader", "getRequestHeader" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            yield c.requestHeaders().get(String.valueOf(args.get(0)).toLowerCase());
                        }
                        yield null;
                    }
                    case "setResponseHeader" -> {
                        if (args.size() >= 2 && args.get(0) != null && args.get(1) != null) {
                            c.setResponseHeader(String.valueOf(args.get(0)), String.valueOf(args.get(1)));
                        }
                        yield null;
                    }
                    case "extractPromptText", "getPromptText" -> c.extractPromptText();
                    case "setPromptText" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            String replacement = String.valueOf(args.get(0));
                            c.transformPromptText(_ -> replacement);
                        }
                        yield null;
                    }
                    case "replacePromptText" -> {
                        if (args.size() >= 2 && args.get(0) != null && args.get(1) != null) {
                            String targetStr = String.valueOf(args.get(0));
                            String replStr = String.valueOf(args.get(1));
                            c.transformPromptText(s -> s.replace(targetStr, replStr));
                        }
                        yield null;
                    }
                    case "reject" -> {
                        int status = args.size() > 0 && args.get(0) instanceof Number n ? n.intValue() : 400;
                        String code = args.size() > 1 && args.get(1) != null ? String.valueOf(args.get(1)) : "plugin_rejected";
                        String msg = args.size() > 2 && args.get(2) != null ? String.valueOf(args.get(2)) : "Rejected by JS plugin";
                        yield PluginResult.reject(status, code, msg);
                    }
                    case "shortCircuit" -> {
                        int status = args.size() > 0 && args.get(0) instanceof Number n ? n.intValue() : 200;
                        Object rawBody = args.size() > 1 ? args.get(1) : "{}";
                        String bodyStr;
                        try {
                            bodyStr = (rawBody instanceof String s) ? s : MAPPER.writeValueAsString(rawBody);
                        } catch (Exception e) {
                            bodyStr = String.valueOf(rawBody);
                        }
                        yield PluginResult.shortCircuit(status, bodyStr, Map.of());
                    }
                    case "bindAuthenticatedKey", "bindAuthKey" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            c.bindAuthenticatedKey(String.valueOf(args.get(0)));
                        }
                        yield null;
                    }
                    case "setEndUserId" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            c.setEndUserId(String.valueOf(args.get(0)));
                        }
                        yield null;
                    }
                    case "setModelAlias" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            c.setModelAlias(String.valueOf(args.get(0)));
                        }
                        yield null;
                    }
                    case "setStreamChunk" -> {
                        if (!args.isEmpty() && args.get(0) != null) {
                            c.setStreamChunk(String.valueOf(args.get(0)));
                        }
                        yield null;
                    }
                    case "putAttributionTag" -> {
                        if (args.size() >= 2 && args.get(0) != null && args.get(1) != null) {
                            c.putAttributionTag(String.valueOf(args.get(0)), String.valueOf(args.get(1)));
                        }
                        yield null;
                    }
                    case "reorderTargetsByIds", "setTargetOrder" -> {
                        if (!args.isEmpty() && args.get(0) instanceof List<?> list) {
                            c.reorderTargetsByIds(list.stream().map(String::valueOf).toList());
                        }
                        yield null;
                    }
                    case "jevDecide" -> {
                        if (args.size() == 1 && args.get(0) instanceof Map<?, ?> qMap) {
                            yield c.jevDecide(null, (Map<String, Object>) qMap);
                        } else if (args.size() >= 2 && args.get(1) instanceof Map<?, ?> qMap) {
                            yield c.jevDecide(args.get(0), (Map<String, Object>) qMap);
                        }
                        yield Map.of();
                    }
                    case "continueExecution" -> PluginResult.Continue.INSTANCE;
                    default -> null;
                };
            }
            if (target instanceof String s) {
                return switch (method) {
                    case "includes", "contains" -> !args.isEmpty() && args.get(0) != null && s.contains(String.valueOf(args.get(0)));
                    case "startsWith" -> !args.isEmpty() && args.get(0) != null && s.startsWith(String.valueOf(args.get(0)));
                    case "endsWith" -> !args.isEmpty() && args.get(0) != null && s.endsWith(String.valueOf(args.get(0)));
                    case "toLowerCase" -> s.toLowerCase();
                    case "toUpperCase" -> s.toUpperCase();
                    case "trim" -> s.trim();
                    case "replace", "replaceAll" -> args.size() >= 2 && args.get(0) != null && args.get(1) != null
                        ? s.replace(String.valueOf(args.get(0)), String.valueOf(args.get(1)))
                        : s;
                    case "indexOf" -> !args.isEmpty() && args.get(0) != null ? (long) s.indexOf(String.valueOf(args.get(0))) : -1L;
                    case "substring", "slice" -> {
                        int start = args.size() > 0 && args.get(0) instanceof Number n ? Math.max(0, Math.min(s.length(), n.intValue())) : 0;
                        int end = args.size() > 1 && args.get(1) instanceof Number n ? Math.max(start, Math.min(s.length(), n.intValue())) : s.length();
                        yield s.substring(start, end);
                    }
                    default -> null;
                };
            }
            if (target instanceof List<?> rawList) {
                List<Object> list = (List<Object>) rawList;
                return switch (method) {
                    case "push", "add" -> {
                        if (!args.isEmpty()) list.add(args.get(0));
                        ctx.markRequestBodyMutated();
                        yield (long) list.size();
                    }
                    case "includes", "contains" -> !args.isEmpty() && list.contains(args.get(0));
                    case "join" -> {
                        String sep = !args.isEmpty() && args.get(0) != null ? String.valueOf(args.get(0)) : ",";
                        yield String.join(sep, list.stream().map(String::valueOf).toList());
                    }
                    default -> null;
                };
            }
            return null;
        }

        private Object evalBinary(String op, Object l, Object r) {
            return switch (op) {
                case "+", "+=" -> {
                    if (l instanceof String || r instanceof String) {
                        yield String.valueOf(l) + String.valueOf(r);
                    }
                    if (l instanceof Number nl && r instanceof Number nr) {
                        if (nl instanceof Double || nr instanceof Double) {
                            yield nl.doubleValue() + nr.doubleValue();
                        }
                        yield nl.longValue() + nr.longValue();
                    }
                    yield 0L;
                }
                case "-", "-=" -> {
                    if (l instanceof Number nl && r instanceof Number nr) {
                        if (nl instanceof Double || nr instanceof Double) {
                            yield nl.doubleValue() - nr.doubleValue();
                        }
                        yield nl.longValue() - nr.longValue();
                    }
                    yield 0L;
                }
                case "*" -> {
                    if (l instanceof Number nl && r instanceof Number nr) {
                        if (nl instanceof Double || nr instanceof Double) {
                            yield nl.doubleValue() * nr.doubleValue();
                        }
                        yield nl.longValue() * nr.longValue();
                    }
                    yield 0L;
                }
                case "/" -> {
                    if (l instanceof Number nl && r instanceof Number nr && nr.doubleValue() != 0.0) {
                        yield nl.doubleValue() / nr.doubleValue();
                    }
                    yield 0.0;
                }
                case "%" -> {
                    if (l instanceof Number nl && r instanceof Number nr && nr.longValue() != 0L) {
                        yield nl.longValue() % nr.longValue();
                    }
                    yield 0L;
                }
                case "===", "==" -> valuesEqual(l, r);
                case "!==", "!=" -> !valuesEqual(l, r);
                case "<" -> compareNumbers(l, r) < 0;
                case "<=" -> compareNumbers(l, r) <= 0;
                case ">" -> compareNumbers(l, r) > 0;
                case ">=" -> compareNumbers(l, r) >= 0;
                default -> false;
            };
        }

        private boolean valuesEqual(Object l, Object r) {
            if (l == r) return true;
            if (l == null || r == null) return false;
            if (l instanceof Number nl && r instanceof Number nr) {
                return Double.compare(nl.doubleValue(), nr.doubleValue()) == 0;
            }
            return Objects.equals(l, r);
        }

        private int compareNumbers(Object l, Object r) {
            if (l instanceof Number nl && r instanceof Number nr) {
                return Double.compare(nl.doubleValue(), nr.doubleValue());
            }
            return String.valueOf(l).compareTo(String.valueOf(r));
        }

        private boolean isTruthy(Object v) {
            if (v == null) return false;
            if (v instanceof Boolean b) return b;
            if (v instanceof Number n) return n.doubleValue() != 0.0;
            if (v instanceof String s) return !s.isEmpty();
            return true;
        }

        @SuppressWarnings("unchecked")
        PluginResult toPluginResult(Object ret) {
            if (ret == null) {
                return PluginResult.Continue.INSTANCE;
            }
            if (ret instanceof PluginResult pr) {
                return pr;
            }
            if (ret instanceof Map<?, ?> m) {
                Object actionObj = m.get("action");
                String action = actionObj != null ? String.valueOf(actionObj).toUpperCase() : "CONTINUE";
                int status = m.get("status") instanceof Number n ? n.intValue() : ("REJECT".equals(action) ? 400 : 200);
                Map<String, String> hdrs = new LinkedHashMap<>();
                if (m.get("headers") instanceof Map<?, ?> hm) {
                    for (Map.Entry<?, ?> e : hm.entrySet()) {
                        if (e.getKey() != null && e.getValue() != null) {
                            hdrs.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                        }
                    }
                }
                if ("REJECT".equals(action)) {
                    String code = m.get("code") != null ? String.valueOf(m.get("code")) : "plugin_rejected";
                    String msg = m.get("message") != null ? String.valueOf(m.get("message")) : "Rejected by JS plugin";
                    return PluginResult.reject(status, code, msg, hdrs);
                }
                if ("SHORT_CIRCUIT".equals(action) || "SHORTCIRCUIT".equals(action)) {
                    Object bodyObj = m.get("body");
                    String bodyStr;
                    try {
                        bodyStr = (bodyObj instanceof String s) ? s : MAPPER.writeValueAsString(bodyObj != null ? bodyObj : Map.of());
                    } catch (Exception e) {
                        bodyStr = String.valueOf(bodyObj);
                    }
                    return PluginResult.shortCircuit(status, bodyStr, hdrs);
                }
            }
            return PluginResult.Continue.INSTANCE;
        }
    }
}
