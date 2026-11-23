package com.jihuayu.aigateway.plugin;

import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ContextRequestEnvelope;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ContextResponseEnvelope;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.HostUpcallRequest;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.HostUpcallResponse;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ManifestEntry;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ManifestPayload;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.PluginKvStore;
import com.jihuayu.aigateway.plugin.PluginExecutionContext.PluginKvStore.KvBackend;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

/**
 * Automatic {@code @CEntryPoint} Wrapper Bridge compiled into {@code --shared} native libraries
 * from unmodified {@link GatewayPlugin} {@code .jar} artifacts (#36).
 *
 * <p>Activated exclusively when {@code -Dai.gateway.shared.plugin.build=true} is set during
 * {@code native-image --shared} compilation, keeping the standalone {@code ai-gateway-netty}
 * executable free of exported C symbols.
 */
public final class SharedLibPluginBridge {

    public static final class SharedPluginBuildPredicate implements BooleanSupplier {
        @Override
        public boolean getAsBoolean() {
            return Boolean.getBoolean("ai.gateway.shared.plugin.build");
        }
    }

    /**
     * Initialized at Native Image build time so the exact target provider class list from the wrapped {@code .jar}
     * is baked into the shared library image heap.
     */
    public static final class BuildTimeTargetConfig {
        public static final String TARGET_CLASSES = System.getProperty("ai.gateway.shared.plugin.targetClasses", "");

        private BuildTimeTargetConfig() {}
    }

    public interface HostUpcallFnPtr extends CFunctionPointer {
        @InvokeCFunctionPointer
        int invoke(
            long callHandle,
            CCharPointer reqPtr,
            long reqLen,
            CCharPointer outBufPtr,
            long outBufCap,
            CLongPointer outActualLenPtr
        );
    }

    private static final int UPCALL_SCRATCH_BYTES = 32_768;

    private static volatile Map<String, GatewayPlugin> cachedPlugins;

    private SharedLibPluginBridge() {}

    private static Map<String, GatewayPlugin> plugins() {
        Map<String, GatewayPlugin> local = cachedPlugins;
        if (local != null) {
            return local;
        }
        synchronized (SharedLibPluginBridge.class) {
            if (cachedPlugins != null) {
                return cachedPlugins;
            }
            Map<String, GatewayPlugin> discovered = new LinkedHashMap<>();
            Set<String> targetFilter = new LinkedHashSet<>();
            String configuredTargets = BuildTimeTargetConfig.TARGET_CLASSES;
            if (configuredTargets != null && !configuredTargets.isBlank()) {
                for (String cn : configuredTargets.split(",")) {
                    String trimmed = cn.trim();
                    if (!trimmed.isEmpty()) {
                        targetFilter.add(trimmed);
                    }
                }
            }

            ServiceLoader<GatewayPlugin> loader = ServiceLoader.load(
                GatewayPlugin.class,
                SharedLibPluginBridge.class.getClassLoader()
            );
            for (GatewayPlugin gp : loader) {
                String clsName = gp.getClass().getName();
                if (!targetFilter.isEmpty() && !targetFilter.contains(clsName)) {
                    continue;
                }
                if (targetFilter.isEmpty() && PiiGuardrailPlugin.class.getName().equals(clsName)) {
                    continue;
                }
                register(discovered, gp);
            }

            for (String targetClass : targetFilter) {
                if (!discovered.containsKey(targetClass)) {
                    try {
                        Class<?> cls = Class.forName(targetClass);
                        if (GatewayPlugin.class.isAssignableFrom(cls)) {
                            GatewayPlugin gp = (GatewayPlugin) cls.getDeclaredConstructor().newInstance();
                            register(discovered, gp);
                        }
                    } catch (Exception ignored) {}
                }
            }

            cachedPlugins = discovered;
            return discovered;
        }
    }

    private static void register(Map<String, GatewayPlugin> map, GatewayPlugin gp) {
        map.put(gp.id(), gp);
        map.put(gp.name(), gp);
        map.put(gp.getClass().getName(), gp);
    }

    @CEntryPoint(name = "ai_gw_plugin_manifest", include = SharedPluginBuildPredicate.class)
    public static CCharPointer pluginManifest(IsolateThread thread, CLongPointer outLenPtr) {
        try {
            Map<String, GatewayPlugin> all = plugins();
            Set<GatewayPlugin> unique = new LinkedHashSet<>(all.values());
            List<ManifestEntry> entries = new ArrayList<>(unique.size());
            for (GatewayPlugin gp : unique) {
                entries.add(new ManifestEntry(
                    gp.id(),
                    gp.name(),
                    gp.type(),
                    gp.priority(),
                    gp.getClass().getName()
                ));
            }
            byte[] bytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(new ManifestPayload(entries));
            return allocateNativeBuffer(bytes, outLenPtr);
        } catch (Throwable t) {
            byte[] err = ("{\"plugins\":[]}").getBytes(StandardCharsets.UTF_8);
            return allocateNativeBuffer(err, outLenPtr);
        }
    }

    @CEntryPoint(name = "ai_gw_plugin_invoke", include = SharedPluginBuildPredicate.class)
    public static CCharPointer pluginInvoke(
        IsolateThread thread,
        CCharPointer pluginIdPtr,
        long pluginIdLen,
        int stageOrdinal,
        CCharPointer ctxPtr,
        long ctxLen,
        HostUpcallFnPtr upcallFn,
        long callHandle,
        CLongPointer outLenPtr
    ) {
        try {
            String pluginId = readUtf8(pluginIdPtr, (int) pluginIdLen);
            GatewayPlugin plugin = plugins().get(pluginId);
            if (plugin == null) {
                byte[] bytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(
                    ContextResponseEnvelope.error("Shared library plugin not found: " + pluginId)
                );
                return allocateNativeBuffer(bytes, outLenPtr);
            }

            PluginStage[] stages = PluginStage.values();
            PluginStage stage = (stageOrdinal >= 0 && stageOrdinal < stages.length)
                ? stages[stageOrdinal]
                : PluginStage.PRE_ADMIT;

            byte[] reqBytes = readBytes(ctxPtr, (int) ctxLen);
            ContextRequestEnvelope reqEnv = FfmPluginProtocol.MAPPER.readValue(reqBytes, ContextRequestEnvelope.class);

            long upcallFnAddr = upcallFn.rawValue();
            PluginKvStore kvStore = createUpcallKvStore(upcallFnAddr, callHandle);
            BiFunction<Object, Map<String, Object>, Map<String, Object>> jevCallback =
                (upcallFnAddr != 0L && callHandle > 0L)
                    ? new UpcallJevCallback(upcallFnAddr, callHandle)
                    : null;
            PluginExecutionContext beforeSnapshot = reqEnv.toExecutionContext(null, null);
            PluginExecutionContext execCtx = reqEnv.toExecutionContext(kvStore, jevCallback);

            PluginResult result = plugin.executeStage(stage, execCtx);
            if (result == null) {
                result = PluginResult.Continue.INSTANCE;
            }
            ContextResponseEnvelope respEnv = ContextResponseEnvelope.fromExecution(beforeSnapshot, execCtx, result);
            byte[] respBytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(respEnv);
            return allocateNativeBuffer(respBytes, outLenPtr);
        } catch (Throwable t) {
            try {
                byte[] errBytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(
                    ContextResponseEnvelope.error(t.getClass().getSimpleName() + ": " + t.getMessage())
                );
                return allocateNativeBuffer(errBytes, outLenPtr);
            } catch (Exception fallbackEx) {
                byte[] raw = "{\"action\":\"CONTINUE\"}".getBytes(StandardCharsets.UTF_8);
                return allocateNativeBuffer(raw, outLenPtr);
            }
        }
    }

    @CEntryPoint(name = "ai_gw_plugin_free", include = SharedPluginBuildPredicate.class)
    public static void pluginFree(IsolateThread thread, CCharPointer ptr) {
        if (ptr.isNonNull()) {
            UnmanagedMemory.free(ptr);
        }
    }

    private static PluginKvStore createUpcallKvStore(long upcallFnAddr, long callHandle) {
        if (upcallFnAddr == 0L || callHandle <= 0L) {
            return new PluginKvStore(null, "gw:");
        }
        return new PluginKvStore(new UpcallKvBackend(upcallFnAddr, callHandle));
    }

    private static final class UpcallKvBackend implements KvBackend {
        private final long upcallFnAddr;
        private final long callHandle;

        UpcallKvBackend(long upcallFnAddr, long callHandle) {
            this.upcallFnAddr = upcallFnAddr;
            this.callHandle = callHandle;
        }

        @Override
        public String get(String key) {
            HostUpcallResponse r = callHost(upcallFnAddr, callHandle, new HostUpcallRequest("KV_GET", key, null, 0L, 0L, null, null));
            return r.strVal();
        }

        @Override
        public void set(String key, String value, long ttlSeconds) {
            callHost(upcallFnAddr, callHandle, new HostUpcallRequest("KV_SET", key, value, 0L, ttlSeconds, null, null));
        }

        @Override
        public boolean setNx(String key, String value, long ttlSeconds) {
            HostUpcallResponse r = callHost(upcallFnAddr, callHandle, new HostUpcallRequest("KV_SET_NX", key, value, 0L, ttlSeconds, null, null));
            return r.boolVal();
        }

        @Override
        public long incr(String key, long delta, long ttlSeconds) {
            HostUpcallResponse r = callHost(upcallFnAddr, callHandle, new HostUpcallRequest("KV_INCR", key, null, delta, ttlSeconds, null, null));
            return r.longVal();
        }

        @Override
        public void del(String key) {
            callHost(upcallFnAddr, callHandle, new HostUpcallRequest("KV_DEL", key, null, 0L, 0L, null, null));
        }
    }

    private static final class UpcallJevCallback implements BiFunction<Object, Map<String, Object>, Map<String, Object>> {
        private final long upcallFnAddr;
        private final long callHandle;

        UpcallJevCallback(long upcallFnAddr, long callHandle) {
            this.upcallFnAddr = upcallFnAddr;
            this.callHandle = callHandle;
        }

        @Override
        public Map<String, Object> apply(Object state, Map<String, Object> questions) {
            HostUpcallResponse r = callHost(
                upcallFnAddr,
                callHandle,
                new HostUpcallRequest("JEV_DECIDE", null, null, 0L, 0L, state, questions)
            );
            return r.mapVal() != null ? r.mapVal() : Map.of();
        }
    }

    private static HostUpcallResponse callHost(long upcallFnAddr, long callHandle, HostUpcallRequest req) {
        HostUpcallFnPtr upcallFn = WordFactory.pointer(upcallFnAddr);
        CCharPointer reqBuf = WordFactory.nullPointer();
        CCharPointer outBuf = WordFactory.nullPointer();
        CLongPointer outLen = WordFactory.nullPointer();
        try {
            byte[] reqBytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(req);
            reqBuf = UnmanagedMemory.malloc(Math.max(1, reqBytes.length));
            CTypeConversion.asByteBuffer(reqBuf, reqBytes.length).put(reqBytes);

            outBuf = UnmanagedMemory.malloc(UPCALL_SCRATCH_BYTES);
            outLen = UnmanagedMemory.malloc(8);
            outLen.write(0L);

            int rc = upcallFn.invoke(callHandle, reqBuf, reqBytes.length, outBuf, UPCALL_SCRATCH_BYTES, outLen);
            int actualLen = (int) outLen.read();
            if (actualLen <= 0 || actualLen > UPCALL_SCRATCH_BYTES) {
                if (rc != 0) {
                    throw new IllegalStateException("Host upcall failed with status " + rc);
                }
                return new HostUpcallResponse(true, null, 0L, false, Map.of(), null);
            }
            byte[] respBytes = readBytes(outBuf, actualLen);
            HostUpcallResponse resp = FfmPluginProtocol.MAPPER.readValue(respBytes, HostUpcallResponse.class);
            if (!resp.ok() && resp.error() != null) {
                throw new IllegalStateException(resp.error());
            }
            return resp;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Host FFM upcall failed: " + e.getMessage(), e);
        } finally {
            if (reqBuf.isNonNull()) UnmanagedMemory.free(reqBuf);
            if (outBuf.isNonNull()) UnmanagedMemory.free(outBuf);
            if (outLen.isNonNull()) UnmanagedMemory.free(outLen);
        }
    }

    private static CCharPointer allocateNativeBuffer(byte[] bytes, CLongPointer outLenPtr) {
        int len = bytes != null ? bytes.length : 0;
        CCharPointer buf = UnmanagedMemory.malloc(Math.max(1, len));
        if (len > 0) {
            CTypeConversion.asByteBuffer(buf, len).put(bytes);
        }
        if (outLenPtr.isNonNull()) {
            outLenPtr.write(len);
        }
        return buf;
    }

    private static byte[] readBytes(CCharPointer ptr, int len) {
        if (ptr.isNull() || len <= 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        CTypeConversion.asByteBuffer(ptr, len).get(out);
        return out;
    }

    private static String readUtf8(CCharPointer ptr, int len) {
        return new String(readBytes(ptr, len), StandardCharsets.UTF_8);
    }
}
