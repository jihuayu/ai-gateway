package com.jihuayu.aigateway.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.hot.HotContract.PluginBindingSnapshot;
import com.jihuayu.aigateway.hot.HotContract.PluginSnapshot;
import com.jihuayu.aigateway.hot.HotContract.Snapshot;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.PluginKvStore;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.PluginKvStore.KvBackend;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end verification of Solution 3 (#36):
 * 1) Plain C shared library (.so) dynamic load, concurrent virtual-thread hot-swap, and deterministic unload.
 * 2) Unmodified standard {@link GatewayPlugin} .jar compiled via {@link JarToSharedLibCompiler}
 *    (GraalVM {@code native-image --shared}) and dynamically loaded, hot-watched, invoked across
 *    virtual threads with live host {@code ctx.kv()} FFM upcalls, and deterministically unloaded.
 */
class NativeFfmPluginLoaderTest {

    private Path workDir;

    @BeforeEach
    void setUpWorkDir() throws Exception {
        // Allocate under project build/ directory rather than java.io.tmpdir (/tmp is often mounted noexec in Linux containers)
        workDir = Files.createDirectories(
            Path.of("build", "ffm-test-work", UUID.randomUUID().toString()).toAbsolutePath().normalize()
        );
    }

    @AfterEach
    void cleanUpWorkDir() {
        if (workDir != null && Files.exists(workDir)) {
            try (var walk = Files.walk(workDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            } catch (Exception ignored) {}
        }
    }

    private static final class InMemoryKvBackend implements KvBackend {
        private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

        @Override
        public String get(String key) {
            return store.get(key);
        }

        @Override
        public void set(String key, String value, long ttlSeconds) {
            if (value == null) {
                store.remove(key);
            } else {
                store.put(key, value);
            }
        }

        @Override
        public boolean setNx(String key, String value, long ttlSeconds) {
            return store.putIfAbsent(key, value) == null;
        }

        @Override
        public long incr(String key, long delta, long ttlSeconds) {
            long next = counters.computeIfAbsent(key, _ -> new AtomicLong(0L)).addAndGet(delta);
            store.put(key, String.valueOf(next));
            return next;
        }

        @Override
        public void del(String key) {
            store.remove(key);
            counters.remove(key);
        }
    }

    @Test
    void plainCNativeSharedLibraryLoadHotSwapAndDeterministicUnload() throws Exception {
        assumeTrue(isCommandAvailable("gcc"), "gcc is required to compile plain C test shared library");

        Path destroyLogV1 = workDir.resolve("destroy-v1.marker");
        Path destroyLogV2 = workDir.resolve("destroy-v2.marker");
        Path soPath = workDir.resolve("libc_test_plugin" + NativeFfmPluginLoader.sharedLibraryExtension());

        compilePlainCPlugin(soPath, "v1", destroyLogV1, 15);

        try (DynamicJarPluginLoader loader = new DynamicJarPluginLoader()) {
            List<GatewayPlugin> v1Plugins = loader.loadNativeLibrary(soPath);
            assertEquals(1, v1Plugins.size());
            GatewayPlugin v1 = loader.getPlugin("native-c:header-guard");
            assertNotNull(v1);
            assertEquals("NATIVE_FFM", v1.type());

            // Verify PRE_ADMIT header mutation on v1
            PluginExecutionContext ctx1 = sampleContext("req-c-1", Map.of(), new InMemoryKvBackend());
            PluginResult res1 = v1.executeStage(PluginStage.PRE_ADMIT, ctx1);
            assertInstanceOf(PluginResult.Continue.class, res1);
            assertEquals("v1", ctx1.responseHeaders().get("x-c-plugin-ver"));

            // Verify REJECT when request header triggers block rule in C code
            PluginExecutionContext blockCtx = sampleContext("req-c-block", Map.of("x-block-me", "1"), new InMemoryKvBackend());
            PluginResult blockRes = v1.executeStage(PluginStage.PRE_ADMIT, blockCtx);
            PluginResult.Reject reject = assertInstanceOf(PluginResult.Reject.class, blockRes);
            assertEquals(403, reject.httpStatus());
            assertEquals("c_native_blocked", reject.errorCode());

            // Start concurrent virtual threads holding in-flight calls in v1 (15ms native sleep per call)
            // while hot-swapping the exact same .so path to v2!
            CountDownLatch enteredLatch = new CountDownLatch(4);
            List<Future<String>> futures = new ArrayList<>();
            try (var vexec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 8; i++) {
                    final int idx = i;
                    futures.add(vexec.submit(() -> {
                        enteredLatch.countDown();
                        PluginExecutionContext c = sampleContext("req-concurrent-" + idx, Map.of("x-slow-ms", "1"), new InMemoryKvBackend());
                        GatewayPlugin current = loader.getPlugin("native-c:header-guard");
                        PluginResult r = current.executeStage(PluginStage.PRE_ADMIT, c);
                        assertInstanceOf(PluginResult.Continue.class, r);
                        return c.responseHeaders().get("x-c-plugin-ver");
                    }));
                }
                assertTrue(enteredLatch.await(5, TimeUnit.SECONDS));

                // Overwrite the .so file on disk with v2 and hot-reload while v1 calls are in flight
                compilePlainCPlugin(soPath, "v2", destroyLogV2, 0);
                Files.setLastModifiedTime(soPath, FileTime.fromMillis(System.currentTimeMillis() + 2000L));
                loader.loadNativeLibrary(soPath);

                for (Future<String> f : futures) {
                    String ver = f.get(10, TimeUnit.SECONDS);
                    assertTrue("v1".equals(ver) || "v2".equals(ver), "Unexpected version: " + ver);
                }
            }

            // Once all in-flight v1 virtual threads finished, v1 must have deterministically run ai_gw_c_plugin_destroy!
            assertTrue(Files.exists(destroyLogV1), "v1 shared library should be deterministically destroyed after draining");
            assertFalse(Files.exists(destroyLogV2), "v2 shared library should still be active");

            // New invocations hit v2
            GatewayPlugin v2 = loader.getPlugin("native-c:header-guard");
            assertNotNull(v2);
            PluginExecutionContext ctx2 = sampleContext("req-c-2", Map.of(), new InMemoryKvBackend());
            v2.executeStage(PluginStage.PRE_ADMIT, ctx2);
            assertEquals("v2", ctx2.responseHeaders().get("x-c-plugin-ver"));

            // Deterministically unload v2
            assertTrue(loader.unloadNativeLibrary(soPath));
            assertTrue(Files.exists(destroyLogV2), "v2 shared library should run destroy hook immediately on unload");
            assertNull(loader.getPlugin("native-c:header-guard"));
            assertTrue(loader.nativeFfmLoader().loadedLibrariesSummary().isEmpty());

            // Calling a retained stale reference after unload fails cleanly instead of crashing JVM/Native host
            assertThrows(IllegalStateException.class, () -> v2.executeStage(PluginStage.PRE_ADMIT, ctx2));
        }
    }

    @Test
    void unmodifiedGatewayPluginJarCompiledToSharedLibAndHotLoadedAndUnloadedViaFfm() throws Exception {
        assumeTrue(
            JarToSharedLibCompiler.isNativeImageToolAvailable(),
            "GraalVM native-image is required for unmodified .jar -> --shared compilation test"
        );

        // 1. Build a 100% unmodified external GatewayPlugin .jar (standard Java SPI, zero GraalVM imports)
        Path unmodifiedJar = buildUnmodifiedExternalPluginJar(workDir.resolve("ext-quota-plugin.jar"));
        JarToSharedLibCompiler.JarInspection inspection = JarToSharedLibCompiler.inspectPluginJar(unmodifiedJar);
        assertEquals(List.of("com.example.extplugin.UnmodifiedQuotaHeaderPlugin"), inspection.providerClassNames());

        // 2. Compile the unmodified .jar into a GraalVM --shared library (.so/.dylib/.dll)
        Path pluginsWatchDir = Files.createDirectories(workDir.resolve("watched-plugins"));
        Path targetSo = pluginsWatchDir.resolve("libext_quota_plugin" + NativeFfmPluginLoader.sharedLibraryExtension());
        Path compiledSo = JarToSharedLibCompiler.compileJarToSharedLib(unmodifiedJar, targetSo);
        assertTrue(Files.isRegularFile(compiledSo), "Compiled --shared plugin library should exist: " + compiledSo);

        InMemoryKvBackend kvBackend = new InMemoryKvBackend();
        ObjectMapper mapper = new ObjectMapper();

        try (PluginPipeline pipeline = new PluginPipeline(null, "gw:", null, mapper)) {
            // 3a. Verify direct FFM Isolate load + deterministic unload first (surfaces exact diagnostics if any)
            List<GatewayPlugin> directLoaded = pipeline.jarLoader().loadNativeLibrary(compiledSo);
            assertEquals(1, directLoaded.size());
            assertTrue(pipeline.jarLoader().unloadNativeLibrary(compiledSo));
            assertNull(pipeline.jarLoader().getPlugin("ext:quota-header"));

            // 3b. Hot-load via DynamicJsPluginManager directory watcher (PLUGINS_DIR/*.so)
            List<String> watchedIds = pipeline.jsManager().scanDirectoryOnce(pluginsWatchDir);
            assertTrue(watchedIds.contains("ext:quota-header"), "Directory watcher should hot-load .so plugin: " + watchedIds);

            GatewayPlugin loaded = pipeline.jarLoader().getPlugin("ext:quota-header");
            assertNotNull(loaded);
            assertEquals("NATIVE_FFM", loaded.type());
            assertEquals(1, pipeline.jarLoader().nativeFfmLoader().loadedLibrariesSummary().size());
            assertEquals(
                "GRAAL_ISOLATE",
                pipeline.jarLoader().nativeFfmLoader().loadedLibrariesSummary().getFirst().abiMode()
            );

            // Bind the NATIVE_FFM plugin into a Snapshot and execute through the full 8-stage PluginPipeline
            Snapshot snapshot = new Snapshot(
                "snap-1",
                Map.of(),
                Map.of(),
                List.of(new PluginSnapshot(
                    "db-plugin-uuid-1",
                    "ws-1",
                    "ext:quota-header",
                    "NATIVE_FFM",
                    1,
                    25,
                    List.of("PRE_ADMIT"),
                    null,
                    null,
                    compiledSo.toString(),
                    "com.example.extplugin.UnmodifiedQuotaHeaderPlugin",
                    Map.of("tagValue", "gold-enterprise", "maxCalls", 5),
                    "FAIL_CLOSED",
                    50,
                    10_000,
                    true
                )),
                List.of(new PluginBindingSnapshot(
                    "bind-1",
                    "db-plugin-uuid-1",
                    "GLOBAL",
                    "*",
                    25,
                    Map.of(),
                    true,
                    1.0
                ))
            );

            // 4. Execute concurrently across Virtual Threads to verify IsolateThread attach/detach & Host KV Upcalls
            try (var vexec = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<PluginResult>> calls = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    final int seq = i;
                    calls.add(vexec.submit(() -> {
                        PluginExecutionContext ctx = sampleContext("req-jar-so-" + seq, Map.of(), kvBackend);
                        PluginResult r = pipeline.executeStage(snapshot, PluginStage.PRE_ADMIT, ctx);
                        assertEquals("true", ctx.requestHeaders().get("x-ext-unmodified-jar"));
                        assertEquals("gold-enterprise", ctx.attributionTags().get("ext_tier"));
                        assertEquals("routed-by-shared-jar", ctx.modelAlias());
                        assertEquals(0.2, ((Number) ctx.requestJson().get("temperature")).doubleValue(), 1e-6);
                        assertFalse(ctx.traceLog().isEmpty());
                        assertEquals("NATIVE_FFM", ctx.traceLog().getFirst().pluginType());
                        return r;
                    }));
                }
                for (Future<PluginResult> f : calls) {
                    assertInstanceOf(PluginResult.Continue.class, f.get(10, TimeUnit.SECONDS));
                }
            }

            // Verify host KV store received all 5 atomic increments via FFM Direct Upcall!
            assertEquals("5", kvBackend.get("calls:key-1"));

            // 6th call exceeds maxCalls=5 -> plugin inside GraalVM Isolate returns PluginResult.reject(429, ...)
            PluginExecutionContext overQuotaCtx = sampleContext("req-jar-so-6", Map.of(), kvBackend);
            PluginResult sixthResult = pipeline.executeStage(snapshot, PluginStage.PRE_ADMIT, overQuotaCtx);
            PluginResult.Reject quotaReject = assertInstanceOf(PluginResult.Reject.class, sixthResult);
            assertEquals(429, quotaReject.httpStatus());
            assertEquals("ext_quota_exceeded", quotaReject.errorCode());

            // Verify ShortCircuit support across FFM boundary
            PluginExecutionContext scCtx = sampleContext("req-jar-so-sc", Map.of("x-short-circuit", "1"), kvBackend);
            PluginResult scResult = pipeline.executeStage(snapshot, PluginStage.PRE_ADMIT, scCtx);
            PluginResult.ShortCircuit sc = assertInstanceOf(PluginResult.ShortCircuit.class, scResult);
            assertEquals(200, sc.httpStatus());
            assertEquals("{\"shortCircuitedBy\":\"shared-jar-isolate\"}", sc.responseBody());
            assertEquals("1", sc.headers().get("x-sc-hdr"));

            // 5. Delete .so from watched directory -> scanDirectoryOnce deterministically tears down Isolate & unloads!
            Files.deleteIfExists(compiledSo);
            List<String> afterDeleteIds = pipeline.jsManager().scanDirectoryOnce(pluginsWatchDir);
            assertFalse(afterDeleteIds.contains("ext:quota-header"));
            assertNull(pipeline.jarLoader().getPlugin("ext:quota-header"));
            assertNull(pipeline.jarLoader().getPlugin("com.example.extplugin.UnmodifiedQuotaHeaderPlugin"));
            assertTrue(pipeline.jarLoader().nativeFfmLoader().loadedLibrariesSummary().isEmpty());

            // Built-in baseline plugins remain completely intact after dynamic unload
            assertNotNull(pipeline.jarLoader().getPlugin(PiiGuardrailPlugin.ID));
        }
    }

    private PluginExecutionContext sampleContext(
        String reqId,
        Map<String, String> extraHeaders,
        KvBackend kvBackend
    ) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-type", "application/json");
        headers.putAll(extraHeaders);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", "support-chat");
        body.put("messages", List.of(Map.of("role", "user", "content", "hello gateway")));
        return new PluginExecutionContext(
            reqId,
            "ws-1",
            "proj-1",
            "key-1",
            "/v1/chat/completions",
            "",
            "support-chat",
            false,
            headers,
            body,
            new PluginKvStore(kvBackend)
        );
    }

    private Path buildUnmodifiedExternalPluginJar(Path outputJar) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "System JavaCompiler is required to compile test plugin JAR");

        Path srcDir = Files.createDirectories(workDir.resolve("ext-plugin-src"));
        Path classesDir = Files.createDirectories(workDir.resolve("ext-plugin-classes"));
        Path pkgDir = Files.createDirectories(srcDir.resolve("com/example/extplugin"));
        Path javaFile = pkgDir.resolve("UnmodifiedQuotaHeaderPlugin.java");

        String source = """
            package com.example.extplugin;

            import com.jihuayu.aigateway.plugin.GatewayPlugin;
            import com.jihuayu.aigateway.plugin.PluginExecutionContext;
            import com.jihuayu.aigateway.plugin.PluginResult;
            import com.jihuayu.aigateway.plugin.PluginStage;
            import java.util.Map;

            public class UnmodifiedQuotaHeaderPlugin implements GatewayPlugin {
                @Override
                public String id() {
                    return "ext:quota-header";
                }

                @Override
                public String name() {
                    return "Unmodified External Quota Plugin";
                }

                @Override
                public int priority() {
                    return 25;
                }

                @Override
                public PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
                    if (stage != PluginStage.PRE_ADMIT) {
                        return PluginResult.Continue.INSTANCE;
                    }
                    if ("1".equals(ctx.requestHeaders().get("x-short-circuit"))) {
                        ctx.setResponseHeader("x-ext-short-circuit", "hit");
                        return PluginResult.shortCircuit(
                            200,
                            "{\\"shortCircuitedBy\\":\\"shared-jar-isolate\\"}",
                            Map.of("x-sc-hdr", "1")
                        );
                    }
                    int maxCalls = 100;
                    Object rawMax = ctx.pluginConfig().get("maxCalls");
                    if (rawMax instanceof Number n) {
                        maxCalls = n.intValue();
                    }
                    String tagVal = String.valueOf(ctx.pluginConfig().getOrDefault("tagValue", "standard"));

                    // Live host KV store access across FFM Direct Upcall!
                    long count = ctx.kv().incr("calls:" + ctx.keyId(), 1L, 60L);
                    ctx.kv().set("last_req", ctx.requestId(), 60L);
                    String echoed = ctx.kv().get("last_req");

                    ctx.setRequestHeader("x-ext-unmodified-jar", "true");
                    ctx.setResponseHeader("x-ext-call-count", String.valueOf(count));
                    ctx.setResponseHeader("x-ext-echo-req", echoed != null ? echoed : "");
                    ctx.putAttributionTag("ext_tier", tagVal);
                    ctx.setModelAlias("routed-by-shared-jar");
                    ctx.requestJson().put("temperature", 0.2);

                    if (count > maxCalls) {
                        return PluginResult.reject(429, "ext_quota_exceeded", "External plugin quota exceeded: " + count);
                    }
                    return PluginResult.Continue.INSTANCE;
                }
            }
            """;
        Files.writeString(javaFile, source, StandardCharsets.UTF_8);

        String cp = JarToSharedLibCompiler.resolveBridgeClasspath();
        int rc = compiler.run(
            null,
            null,
            null,
            "--release",
            "25",
            "--enable-preview",
            "-cp",
            cp,
            "-d",
            classesDir.toString(),
            javaFile.toString()
        );
        assertEquals(0, rc, "Failed to javac external test GatewayPlugin");

        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(outputJar))) {
            JarEntry spiEntry = new JarEntry("META-INF/services/com.jihuayu.aigateway.plugin.GatewayPlugin");
            jos.putNextEntry(spiEntry);
            jos.write("com.example.extplugin.UnmodifiedQuotaHeaderPlugin\n".getBytes(StandardCharsets.UTF_8));
            jos.closeEntry();

            Path compiledClass = classesDir.resolve("com/example/extplugin/UnmodifiedQuotaHeaderPlugin.class");
            JarEntry clsEntry = new JarEntry("com/example/extplugin/UnmodifiedQuotaHeaderPlugin.class");
            jos.putNextEntry(clsEntry);
            jos.write(Files.readAllBytes(compiledClass));
            jos.closeEntry();
        }
        return outputJar;
    }

    private void compilePlainCPlugin(Path outSo, String versionTag, Path destroyMarkerPath, int slowSleepMs) throws Exception {
        Path cFile = workDir.resolve("plugin_" + versionTag + ".c");
        String escapedMarker = destroyMarkerPath.toAbsolutePath().toString().replace("\\", "\\\\");
        String cSource = """
            #include <stdio.h>
            #include <stdlib.h>
            #include <string.h>
            #include <time.h>

            const char* ai_gw_c_plugin_manifest(long* out_len) {
                const char* json = "{\\"plugins\\":[{\\"id\\":\\"native-c:header-guard\\",\\"name\\":\\"C Header Guard\\",\\"type\\":\\"NATIVE_FFM\\",\\"priority\\":15,\\"className\\":\\"c.header_guard\\"}]}";
                size_t len = strlen(json);
                char* buf = (char*)malloc(len + 1);
                memcpy(buf, json, len + 1);
                *out_len = (long)len;
                return buf;
            }

            const char* ai_gw_c_plugin_invoke(
                const char* plugin_id,
                long plugin_id_len,
                int stage_ord,
                const char* ctx_json,
                long ctx_len,
                void* upcall_fn,
                long call_handle,
                long* out_len
            ) {
                char* req = (char*)malloc((size_t)ctx_len + 1);
                memcpy(req, ctx_json, (size_t)ctx_len);
                req[ctx_len] = '\\0';

                if (%d > 0 && strstr(req, "\\"x-slow-ms\\"") != NULL) {
                    struct timespec ts;
                    ts.tv_sec = 0;
                    ts.tv_nsec = %dL * 1000000L;
                    nanosleep(&ts, NULL);
                }

                const char* resp;
                if (strstr(req, "\\"x-block-me\\":\\"1\\"") != NULL) {
                    resp = "{\\"action\\":\\"REJECT\\",\\"httpStatus\\":403,\\"errorCode\\":\\"c_native_blocked\\",\\"message\\":\\"Blocked by C FFM plugin\\"}";
                } else {
                    resp = "{\\"action\\":\\"CONTINUE\\",\\"responseHeaders\\":{\\"x-c-plugin-ver\\":\\"%s\\"}}";
                }
                free(req);

                size_t rlen = strlen(resp);
                char* out = (char*)malloc(rlen + 1);
                memcpy(out, resp, rlen + 1);
                *out_len = (long)rlen;
                return out;
            }

            void ai_gw_c_plugin_free(void* ptr) {
                if (ptr != NULL) {
                    free(ptr);
                }
            }

            void ai_gw_c_plugin_destroy(void) {
                FILE* f = fopen("%s", "w");
                if (f != NULL) {
                    fputs("destroyed:%s\\n", f);
                    fclose(f);
                }
            }
            """.formatted(slowSleepMs, slowSleepMs, versionTag, escapedMarker, versionTag);
        Files.writeString(cFile, cSource, StandardCharsets.UTF_8);

        Path tmpOut = workDir.resolve("tmp_" + versionTag + NativeFfmPluginLoader.sharedLibraryExtension());
        Process proc = new ProcessBuilder("gcc", "-shared", "-fPIC", "-O2", cFile.toString(), "-o", tmpOut.toString())
            .redirectErrorStream(true)
            .start();
        String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int code = proc.waitFor();
        assertEquals(0, code, "gcc failed:\n" + out);
        Files.move(tmpOut, outSo, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static boolean isCommandAvailable(String cmd) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return false;
        for (String dir : pathEnv.split(File.pathSeparator)) {
            if (dir == null || dir.isBlank()) continue;
            if (Files.isExecutable(Path.of(dir, cmd))) {
                return true;
            }
        }
        return false;
    }
}
