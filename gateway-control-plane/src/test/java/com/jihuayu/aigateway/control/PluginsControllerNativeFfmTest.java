package com.jihuayu.aigateway.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihuayu.aigateway.control.ControlAuthService.SessionInfo;
import com.jihuayu.aigateway.hot.GatewayException;
import com.jihuayu.aigateway.hot.HotRuntime;
import com.jihuayu.aigateway.netty.DataPlaneExecutor;
import com.jihuayu.aigateway.plugin.NativeFfmPluginLoader;
import com.jihuayu.aigateway.plugin.PluginPipeline;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class PluginsControllerNativeFfmTest {

    private static final String C_PLUGIN_SOURCE = """
        #include <stdlib.h>
        #include <string.h>

        const char* ai_gw_c_plugin_manifest(long* out_len) {
            const char* m = "{\\"plugins\\":[{\\"id\\":\\"ctrl-native-c\\",\\"name\\":\\"Controller Native C\\",\\"type\\":\\"NATIVE_FFM\\",\\"priority\\":15,\\"className\\":\\"native.ctrl.Plugin\\"}]}";
            size_t len = strlen(m);
            char* out = (char*)malloc(len + 1);
            memcpy(out, m, len + 1);
            *out_len = (long)len;
            return out;
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
            (void)plugin_id;
            (void)plugin_id_len;
            (void)stage_ord;
            (void)upcall_fn;
            (void)call_handle;
            char* req = (char*)malloc((size_t)ctx_len + 1);
            memcpy(req, ctx_json, (size_t)ctx_len);
            req[ctx_len] = '\\0';
            const char* resp;
            if (strstr(req, "deny-me") != NULL) {
                resp = "{\\"action\\":\\"REJECT\\",\\"httpStatus\\":403,\\"errorCode\\":\\"ctrl_blocked\\",\\"message\\":\\"Blocked by native C plugin\\"}";
            } else {
                resp = "{\\"action\\":\\"CONTINUE\\",\\"requestHeaders\\":{\\"x-ctrl-native\\":\\"active\\"},\\"attributionTags\\":{\\"native_ctrl\\":\\"yes\\"}}";
            }
            free(req);
            size_t len = strlen(resp);
            char* out = (char*)malloc(len + 1);
            memcpy(out, resp, len + 1);
            *out_len = (long)len;
            return out;
        }

        void ai_gw_c_plugin_free(void* ptr) {
            free(ptr);
        }
        """;

    @Test
    @SuppressWarnings("unchecked")
    void loadListDryRunAndUnloadNativeLibraryViaControllerEndpoints() throws Exception {
        Path workDir = Files.createTempDirectory(
            Files.createDirectories(Path.of("build/ffm-ctrl-test")),
            "ctrl-ffm-"
        );
        Path cFile = workDir.resolve("ctrl_plugin.c");
        Path soFile = workDir.resolve("libctrl_plugin.so");
        Files.writeString(cFile, C_PLUGIN_SOURCE, StandardCharsets.UTF_8);

        Process gcc = new ProcessBuilder(
            "gcc", "-shared", "-fPIC", "-O2", "-o", soFile.toString(), cFile.toString()
        ).redirectErrorStream(true).start();
        String gccOut = new String(gcc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, gcc.waitFor(), "gcc failed: " + gccOut);

        ObjectMapper mapper = new ObjectMapper();
        PluginPipeline pipeline = new PluginPipeline(null, "test-kv:", null, mapper);

        DataSource dataSource = mock(DataSource.class);
        ControlAuthService authService = mock(ControlAuthService.class);
        HotRuntime hotRuntime = mock(HotRuntime.class);
        DataPlaneExecutor dataPlaneExecutor = mock(DataPlaneExecutor.class);
        HttpServletRequest req = mock(HttpServletRequest.class);

        when(authService.requireWorkspaceRole(any(), anyList()))
            .thenReturn(new SessionInfo("user-1", "ws-1", "admin", List.of("proj-1"), "csrf"));
        when(dataPlaneExecutor.pluginPipeline()).thenReturn(pipeline);

        PluginsController controller = new PluginsController(
            dataSource, authService, hotRuntime, dataPlaneExecutor, mapper
        );

        try {
            // 1. Initially empty
            Map<String, Object> before = controller.listLoadedNativeLibraries(req);
            assertTrue(before.containsKey("nativeImageRuntime"));
            assertTrue(before.containsKey("nativeImageCompilerAvailable"));
            assertTrue(((List<?>) before.get("items")).isEmpty());

            // 2. Missing libPath / jarPath -> 400
            GatewayException missingErr = assertThrows(
                GatewayException.class,
                () -> controller.loadNativePlugin(req, Map.of())
            );
            assertEquals(400, missingErr.getStatusCode());

            // 3. Load shared library via /api/plugins/load-native
            Map<String, Object> loaded = controller.loadNativePlugin(
                req, Map.of("libPath", soFile.toString())
            );
            assertEquals(1, loaded.get("loadedCount"));
            assertEquals(soFile.toAbsolutePath().normalize().toString(), loaded.get("libraryPath"));

            // 4. Verify /api/plugins/native-loaded reports mounted library
            Map<String, Object> afterLoad = controller.listLoadedNativeLibraries(req);
            List<NativeFfmPluginLoader.LoadedNativeLibSummary> items =
                (List<NativeFfmPluginLoader.LoadedNativeLibSummary>) afterLoad.get("items");
            assertEquals(1, items.size());
            assertEquals("PLAIN_C", items.getFirst().abiMode());
            assertEquals(List.of("ctrl-native-c"), items.getFirst().pluginIds());

            // 5. Dry-run mounted native plugin (CONTINUE + REJECT)
            Map<String, Object> dryContinue = controller.dryRunPlugin(req, Map.of(
                "pluginId", "ctrl-native-c",
                "stage", "PRE_ADMIT",
                "headers", Map.of("x-tenant", "demo"),
                "requestBody", Map.of("model", "support-chat")
            ));
            assertEquals("CONTINUE", dryContinue.get("action"));
            assertEquals("active", ((Map<String, String>) dryContinue.get("mutatedRequestHeaders")).get("x-ctrl-native"));
            assertEquals("yes", ((Map<String, String>) dryContinue.get("attributionTags")).get("native_ctrl"));

            Map<String, Object> dryReject = controller.dryRunPlugin(req, Map.of(
                "pluginId", "ctrl-native-c",
                "stage", "PRE_ADMIT",
                "headers", Map.of("x-flag", "deny-me"),
                "requestBody", Map.of("model", "support-chat")
            ));
            assertEquals("REJECT", dryReject.get("action"));
            assertEquals(403, dryReject.get("httpStatus"));
            assertEquals("ctrl_blocked", dryReject.get("errorCode"));

            // 6. Unload via /api/plugins/unload-native
            Map<String, Object> unloaded = controller.unloadNativePlugin(
                req, Map.of("libPath", soFile.toString())
            );
            assertEquals(true, unloaded.get("unloaded"));

            Map<String, Object> afterUnload = controller.listLoadedNativeLibraries(req);
            assertTrue(((List<?>) afterUnload.get("items")).isEmpty());

            // Unloading again is idempotent (false)
            Map<String, Object> secondUnload = controller.unloadNativePlugin(
                req, Map.of("pluginId", "ctrl-native-c")
            );
            assertFalse((Boolean) secondUnload.get("unloaded"));
        } finally {
            pipeline.jarLoader().nativeFfmLoader().close();
        }
    }
}
