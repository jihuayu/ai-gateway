package com.jihuayu.aigateway.plugin;

import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ContextRequestEnvelope;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ContextResponseEnvelope;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.HostUpcallRequest;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.HostUpcallResponse;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ManifestEntry;
import com.jihuayu.aigateway.plugin.FfmPluginProtocol.ManifestPayload;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * JDK 25 Foreign Function &amp; Memory (FFM) Dynamic Shared-Library Plugin Loader &amp; Unloader (#36).
 *
 * <p>Works identically in both JVM mode and Oracle GraalVM 25 Native Image mode without breaking
 * the 49 MB zero-SQL data-plane footprint:
 * <ul>
 *   <li><b>GraalVM {@code --shared} Isolate ABI</b>: Dynamically loads {@code .so}/{@code .dylib}/{@code .dll}
 *       shared libraries compiled from unmodified {@link GatewayPlugin} {@code .jar} plugins via
 *       {@link JarToSharedLibCompiler}, managing dedicated {@code graal_isolate_t} heaps and virtual-thread
 *       safe carrier attach/detach.</li>
 *   <li><b>Plain C/Rust Native ABI</b>: Also supports lightweight C/Rust/Zig shared libraries exporting
 *       {@code ai_gw_c_plugin_manifest}, {@code ai_gw_c_plugin_invoke}, and {@code ai_gw_c_plugin_free}.</li>
 *   <li><b>Copy-on-Load Staging &amp; Reference-Counted Deterministic Unload</b>: Copies each library to an
 *       executable staging directory next to the library (avoiding hardened container {@code /tmp} {@code noexec}
 *       mounts) before {@code dlopen}, drains in-flight requests on hot-swap or unload, tears down the GraalVM
 *       Isolate ({@code graal_tear_down_isolate}), and closes {@link Arena#ofShared()}.</li>
 * </ul>
 */
public class NativeFfmPluginLoader implements AutoCloseable {

    public enum AbiMode {
        GRAAL_ISOLATE,
        PLAIN_C
    }

    public record LoadedNativeLibSummary(
        String libraryPath,
        String abiMode,
        long lastModifiedMillis,
        long loadedAtMillis,
        int inFlightCalls,
        List<String> pluginIds
    ) {}

    private static final ConcurrentHashMap<Long, PluginExecutionContext> UPCALL_CONTEXTS = new ConcurrentHashMap<>();
    private static final AtomicLong CALL_HANDLE_SEQ = new AtomicLong(1L);
    private static final AtomicLong STAGING_FILE_SEQ = new AtomicLong(1L);

    private final Map<Path, LoadedLibraryHandle> loadedByPath = new ConcurrentHashMap<>();
    private final Map<String, GatewayPlugin> activePlugins = new ConcurrentHashMap<>();
    private final Map<String, Path> pluginKeyToLibPath = new ConcurrentHashMap<>();
    private final BiConsumer<String, GatewayPlugin> onRegisterHook;
    private final Consumer<String> onUnregisterHook;

    private volatile Linker cachedLinker;
    private volatile MemorySegment cachedUpcallStub;

    public NativeFfmPluginLoader() {
        this(null, null);
    }

    public NativeFfmPluginLoader(
        BiConsumer<String, GatewayPlugin> onRegisterHook,
        Consumer<String> onUnregisterHook
    ) {
        this.onRegisterHook = onRegisterHook;
        this.onUnregisterHook = onUnregisterHook;
    }

    public static boolean isSharedLibraryFileName(String fileName) {
        if (fileName == null) return false;
        String lower = fileName.toLowerCase(Locale.ROOT);
        return lower.endsWith(".so") || lower.endsWith(".dylib") || lower.endsWith(".dll");
    }

    public static String sharedLibraryExtension() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        if (os.contains("win")) {
            return ".dll";
        }
        return ".so";
    }

    /**
     * Resolves a writable staging directory on an executable mount (avoiding {@code /tmp} {@code noexec} mounts).
     */
    static Path resolveStagingDirectory(Path normalizedLibPath) throws Exception {
        String customDir = System.getenv("AI_GATEWAY_FFM_STAGING_DIR");
        if (customDir != null && !customDir.isBlank()) {
            Path p = Path.of(customDir.trim()).toAbsolutePath().normalize();
            Files.createDirectories(p);
            return p;
        }
        Path parent = normalizedLibPath.getParent();
        if (parent != null) {
            try {
                Path siblingStaging = parent.resolve(".ai-gw-ffm-staging");
                Files.createDirectories(siblingStaging);
                if (Files.isWritable(siblingStaging)) {
                    return siblingStaging;
                }
            } catch (Exception ignored) {}
        }
        Path cwdStaging = Path.of(".ai-gw-ffm-staging").toAbsolutePath().normalize();
        Files.createDirectories(cwdStaging);
        return cwdStaging;
    }

    /**
     * Direct FFM Upcall target invoked by {@code --shared} plugins to access host {@code ctx.kv()} and {@code ctx.jevDecide(...)}.
     * Registered in {@code reachability-metadata.json} under {@code directUpcalls}.
     */
    public static int onHostUpcall(
        long callHandle,
        MemorySegment reqPtr,
        long reqLen,
        MemorySegment outBufPtr,
        long outBufCap,
        MemorySegment outActualLenPtr
    ) {
        try {
            PluginExecutionContext ctx = UPCALL_CONTEXTS.get(callHandle);
            if (ctx == null) {
                return writeUpcallResponse(
                    new HostUpcallResponse(false, null, 0L, false, null, "Expired or invalid FFM call handle"),
                    outBufPtr,
                    outBufCap,
                    outActualLenPtr
                );
            }
            byte[] reqBytes = reqPtr.reinterpret(reqLen).toArray(ValueLayout.JAVA_BYTE);
            HostUpcallRequest req = FfmPluginProtocol.MAPPER.readValue(reqBytes, HostUpcallRequest.class);
            String op = req.op() != null ? req.op().toUpperCase(Locale.ROOT) : "";
            HostUpcallResponse resp = switch (op) {
                case "KV_GET" -> new HostUpcallResponse(true, ctx.kv().get(req.key()), 0L, false, null, null);
                case "KV_SET" -> {
                    ctx.kv().set(req.key(), req.value(), req.ttlSeconds());
                    yield new HostUpcallResponse(true, null, 0L, true, null, null);
                }
                case "KV_SET_NX" -> {
                    boolean ok = ctx.kv().setNx(req.key(), req.value(), req.ttlSeconds());
                    yield new HostUpcallResponse(true, null, 0L, ok, null, null);
                }
                case "KV_INCR" -> {
                    long next = ctx.kv().incr(req.key(), req.delta(), req.ttlSeconds());
                    yield new HostUpcallResponse(true, String.valueOf(next), next, true, null, null);
                }
                case "KV_DEL" -> {
                    ctx.kv().del(req.key());
                    yield new HostUpcallResponse(true, null, 0L, true, null, null);
                }
                case "JEV_DECIDE" -> {
                    Map<String, Object> jev = ctx.jevDecide(req.jevState(), req.jevQuestions());
                    yield new HostUpcallResponse(true, null, 0L, true, jev, null);
                }
                default -> new HostUpcallResponse(false, null, 0L, false, null, "Unsupported host upcall op: " + op);
            };
            return writeUpcallResponse(resp, outBufPtr, outBufCap, outActualLenPtr);
        } catch (Throwable t) {
            try {
                writeUpcallResponse(
                    new HostUpcallResponse(false, null, 0L, false, null, t.getMessage()),
                    outBufPtr,
                    outBufCap,
                    outActualLenPtr
                );
            } catch (Exception ignored) {}
            return -1;
        }
    }

    private static int writeUpcallResponse(
        HostUpcallResponse resp,
        MemorySegment outBufPtr,
        long outBufCap,
        MemorySegment outActualLenPtr
    ) throws Exception {
        byte[] bytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(resp);
        MemorySegment lenSeg = outActualLenPtr.reinterpret(ValueLayout.JAVA_LONG.byteSize());
        if (bytes.length > outBufCap) {
            lenSeg.set(ValueLayout.JAVA_LONG, 0L, 0L);
            return -2;
        }
        MemorySegment outSeg = outBufPtr.reinterpret(outBufCap);
        MemorySegment.copy(bytes, 0, outSeg, ValueLayout.JAVA_BYTE, 0L, bytes.length);
        lenSeg.set(ValueLayout.JAVA_LONG, 0L, bytes.length);
        return resp.ok() ? 0 : 1;
    }

    private Linker linker() {
        Linker l = cachedLinker;
        if (l == null) {
            synchronized (this) {
                if (cachedLinker == null) {
                    cachedLinker = Linker.nativeLinker();
                }
                l = cachedLinker;
            }
        }
        return l;
    }

    private MemorySegment hostUpcallStub() {
        MemorySegment stub = cachedUpcallStub;
        if (stub == null) {
            synchronized (this) {
                if (cachedUpcallStub == null) {
                    try {
                        FunctionDescriptor desc = FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS
                        );
                        MethodHandle mh = MethodHandles.lookup().findStatic(
                            NativeFfmPluginLoader.class,
                            "onHostUpcall",
                            MethodType.methodType(
                                int.class,
                                long.class,
                                MemorySegment.class,
                                long.class,
                                MemorySegment.class,
                                long.class,
                                MemorySegment.class
                            )
                        );
                        cachedUpcallStub = linker().upcallStub(mh, desc, Arena.ofAuto());
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to initialize FFM host upcall stub: " + e.getMessage(), e);
                    }
                }
                stub = cachedUpcallStub;
            }
        }
        return stub;
    }

    /**
     * Dynamically loads (or atomically hot-swaps) a {@code .so}/{@code .dylib}/{@code .dll} plugin shared library.
     */
    public synchronized List<GatewayPlugin> loadSharedLibrary(Path libPath) {
        if (libPath == null || !Files.isRegularFile(libPath)) {
            throw new IllegalArgumentException("Native plugin shared library does not exist: " + libPath);
        }
        Path normalized = libPath.toAbsolutePath().normalize();
        String fileName = normalized.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String stem = dot > 0 ? fileName.substring(0, dot) : fileName;
        String suffix = dot >= 0 ? fileName.substring(dot) : sharedLibraryExtension();

        Path stagedFile = null;
        Arena libArena = null;
        try {
            long mtime = Files.getLastModifiedTime(normalized).toMillis();
            Path stagingDir = resolveStagingDirectory(normalized);
            stagedFile = stagingDir.resolve(
                stem + "-v" + STAGING_FILE_SEQ.getAndIncrement() + "-" + System.nanoTime() + suffix
            );
            Files.copy(normalized, stagedFile, StandardCopyOption.REPLACE_EXISTING);

            libArena = Arena.ofShared();
            SymbolLookup lookup = SymbolLookup.libraryLookup(stagedFile, libArena);
            Linker nativeLinker = linker();

            LoadedLibraryHandle newHandle;
            if (lookup.find("graal_create_isolate").isPresent() && lookup.find("ai_gw_plugin_manifest").isPresent()) {
                newHandle = initializeGraalIsolateLibrary(normalized, stagedFile, mtime, libArena, lookup, nativeLinker);
            } else if (lookup.find("ai_gw_c_plugin_manifest").isPresent() && lookup.find("ai_gw_c_plugin_invoke").isPresent()) {
                newHandle = initializePlainCLibrary(normalized, stagedFile, mtime, libArena, lookup, nativeLinker);
            } else {
                throw new IllegalArgumentException(
                    "Shared library '" + normalized + "' does not export AI Gateway plugin symbols "
                        + "(expected ai_gw_plugin_manifest / graal_create_isolate or ai_gw_c_plugin_manifest)"
                );
            }

            List<GatewayPlugin> adapters = new ArrayList<>(newHandle.manifestEntries.size());
            List<String> registeredKeys = new ArrayList<>();
            for (ManifestEntry entry : newHandle.manifestEntries) {
                FfmGatewayPluginAdapter adapter = new FfmGatewayPluginAdapter(entry, newHandle, hostUpcallStub());
                adapters.add(adapter);
                for (String key : keysForEntry(entry)) {
                    activePlugins.put(key, adapter);
                    pluginKeyToLibPath.put(key, normalized);
                    registeredKeys.add(key);
                    if (onRegisterHook != null) {
                        onRegisterHook.accept(key, adapter);
                    }
                }
            }
            newHandle.registeredLookupKeys.addAll(registeredKeys);

            LoadedLibraryHandle previous = loadedByPath.put(normalized, newHandle);
            if (previous != null) {
                for (String oldKey : previous.registeredLookupKeys) {
                    if (!registeredKeys.contains(oldKey)) {
                        activePlugins.remove(oldKey, previous.adaptersByKey.get(oldKey));
                        pluginKeyToLibPath.remove(oldKey, normalized);
                        if (onUnregisterHook != null) {
                            onUnregisterHook.accept(oldKey);
                        }
                    }
                }
                previous.retire();
            }
            for (GatewayPlugin gp : adapters) {
                for (String k : registeredKeys) {
                    if (activePlugins.get(k) == gp) {
                        newHandle.adaptersByKey.put(k, gp);
                    }
                }
            }
            return adapters;
        } catch (Throwable t) {
            if (libArena != null) {
                try { libArena.close(); } catch (Exception ignored) {}
            }
            if (stagedFile != null) {
                try { Files.deleteIfExists(stagedFile); } catch (Exception ignored) {}
            }
            throw new IllegalStateException("Failed to load native FFM plugin library " + normalized + ": " + t.getMessage(), t);
        }
    }

    /**
     * Dynamically unloads a previously mounted shared library, draining any in-flight calls before
     * tearing down its GraalVM Isolate and closing its {@link Arena}.
     */
    public synchronized boolean unloadSharedLibrary(Path libPath) {
        if (libPath == null) return false;
        Path normalized = libPath.toAbsolutePath().normalize();
        LoadedLibraryHandle handle = loadedByPath.remove(normalized);
        if (handle == null) {
            return false;
        }
        for (String key : handle.registeredLookupKeys) {
            GatewayPlugin adapter = handle.adaptersByKey.get(key);
            if (adapter != null) {
                activePlugins.remove(key, adapter);
            } else {
                activePlugins.remove(key);
            }
            pluginKeyToLibPath.remove(key, normalized);
            if (onUnregisterHook != null) {
                onUnregisterHook.accept(key);
            }
        }
        handle.retire();
        return true;
    }

    /**
     * Unloads the shared library owning the specified plugin ID, name, or implementation class name.
     */
    public synchronized boolean unloadPluginById(String idOrClassName) {
        if (idOrClassName == null || idOrClassName.isBlank()) return false;
        Path path = pluginKeyToLibPath.get(idOrClassName.trim());
        if (path == null) {
            return false;
        }
        return unloadSharedLibrary(path);
    }

    public boolean isLibraryLoaded(Path libPath) {
        if (libPath == null) return false;
        return loadedByPath.containsKey(libPath.toAbsolutePath().normalize());
    }

    public boolean isLibraryModified(Path libPath, long currentMtime) {
        if (libPath == null) return false;
        LoadedLibraryHandle h = loadedByPath.get(libPath.toAbsolutePath().normalize());
        return h == null || h.lastModifiedMillis != currentMtime;
    }

    public GatewayPlugin getPlugin(String idOrClassName) {
        if (idOrClassName == null) return null;
        return activePlugins.get(idOrClassName);
    }

    public Map<String, GatewayPlugin> allPlugins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(activePlugins));
    }

    public List<LoadedNativeLibSummary> loadedLibrariesSummary() {
        List<LoadedNativeLibSummary> list = new ArrayList<>();
        for (LoadedLibraryHandle h : loadedByPath.values()) {
            List<String> ids = new ArrayList<>();
            for (ManifestEntry e : h.manifestEntries) {
                ids.add(e.id());
            }
            list.add(new LoadedNativeLibSummary(
                h.originalPath.toString(),
                h.abiMode.name(),
                h.lastModifiedMillis,
                h.loadedAtMillis,
                Math.max(0, h.refCount.get() - (h.retired.get() ? 0 : 1)),
                ids
            ));
        }
        return list;
    }

    @Override
    public synchronized void close() {
        for (Path p : new ArrayList<>(loadedByPath.keySet())) {
            unloadSharedLibrary(p);
        }
    }

    private static List<String> keysForEntry(ManifestEntry entry) {
        List<String> keys = new ArrayList<>(3);
        if (entry.id() != null && !entry.id().isBlank()) keys.add(entry.id());
        if (entry.name() != null && !entry.name().isBlank() && !keys.contains(entry.name())) keys.add(entry.name());
        if (entry.className() != null && !entry.className().isBlank() && !keys.contains(entry.className())) {
            keys.add(entry.className());
        }
        return keys;
    }

    private LoadedLibraryHandle initializeGraalIsolateLibrary(
        Path originalPath,
        Path stagedPath,
        long mtime,
        Arena libArena,
        SymbolLookup lookup,
        Linker nativeLinker
    ) throws Throwable {
        MethodHandle createIsolate = nativeLinker.downcallHandle(
            lookup.find("graal_create_isolate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );
        MethodHandle attachThread = nativeLinker.downcallHandle(
            lookup.find("graal_attach_thread").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );
        MethodHandle detachThread = nativeLinker.downcallHandle(
            lookup.find("graal_detach_thread").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
        );
        MethodHandle tearDownIsolate = nativeLinker.downcallHandle(
            lookup.find("graal_tear_down_isolate").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
        );
        Optional<MemorySegment> currentThreadSym = lookup.find("graal_get_current_thread");
        MethodHandle getCurrentThread = currentThreadSym.isPresent()
            ? nativeLinker.downcallHandle(
                currentThreadSym.get(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            )
            : null;

        MethodHandle manifestFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_plugin_manifest").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );
        MethodHandle invokeFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_plugin_invoke").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        MethodHandle freeFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_plugin_free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );

        MemorySegment isolateAddr;
        ManifestPayload manifest;
        try (Arena initArena = Arena.ofConfined()) {
            MemorySegment isoPtrBox = initArena.allocate(ValueLayout.ADDRESS);
            MemorySegment thrPtrBox = initArena.allocate(ValueLayout.ADDRESS);
            int rc = (int) createIsolate.invokeExact(MemorySegment.NULL, isoPtrBox, thrPtrBox);
            if (rc != 0) {
                throw new IllegalStateException("graal_create_isolate failed with code " + rc);
            }
            isolateAddr = isoPtrBox.get(ValueLayout.ADDRESS, 0L);
            MemorySegment initThread = thrPtrBox.get(ValueLayout.ADDRESS, 0L);
            try {
                MemorySegment outLenSeg = initArena.allocate(ValueLayout.JAVA_LONG);
                MemorySegment manifestPtr = (MemorySegment) manifestFn.invokeExact(initThread, outLenSeg);
                try {
                    long len = outLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                    byte[] bytes = manifestPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
                    manifest = FfmPluginProtocol.MAPPER.readValue(bytes, ManifestPayload.class);
                } finally {
                    if (!MemorySegment.NULL.equals(manifestPtr)) {
                        freeFn.invokeExact(initThread, manifestPtr);
                    }
                }
            } finally {
                // Detach initialization thread so virtual-thread carriers never collide and teardown never blocks
                int detachRc = (int) detachThread.invokeExact(initThread);
                if (detachRc != 0) {
                    throw new IllegalStateException("graal_detach_thread failed with code " + detachRc);
                }
            }
        }

        List<ManifestEntry> entries = (manifest != null && manifest.plugins() != null) ? manifest.plugins() : List.of();
        if (entries.isEmpty()) {
            try (Arena cleanupArena = Arena.ofConfined()) {
                MemorySegment thrBox = cleanupArena.allocate(ValueLayout.ADDRESS);
                if ((int) attachThread.invokeExact(isolateAddr, thrBox) == 0) {
                    int unusedTearDownRc = (int) tearDownIsolate.invokeExact(thrBox.get(ValueLayout.ADDRESS, 0L));
                }
            }
            throw new IllegalStateException("Shared library exported an empty GatewayPlugin manifest: " + originalPath);
        }

        return new LoadedLibraryHandle(
            originalPath,
            stagedPath,
            mtime,
            AbiMode.GRAAL_ISOLATE,
            libArena,
            entries,
            isolateAddr,
            attachThread,
            detachThread,
            getCurrentThread,
            tearDownIsolate,
            invokeFn,
            freeFn,
            null
        );
    }

    private LoadedLibraryHandle initializePlainCLibrary(
        Path originalPath,
        Path stagedPath,
        long mtime,
        Arena libArena,
        SymbolLookup lookup,
        Linker nativeLinker
    ) throws Throwable {
        MethodHandle manifestFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_c_plugin_manifest").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );
        MethodHandle invokeFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_c_plugin_invoke").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        MethodHandle freeFn = nativeLinker.downcallHandle(
            lookup.find("ai_gw_c_plugin_free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
        );
        Optional<MemorySegment> destroySym = lookup.find("ai_gw_c_plugin_destroy");
        MethodHandle destroyFn = destroySym.isPresent()
            ? nativeLinker.downcallHandle(destroySym.get(), FunctionDescriptor.ofVoid())
            : null;

        ManifestPayload manifest;
        try (Arena initArena = Arena.ofConfined()) {
            MemorySegment outLenSeg = initArena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment manifestPtr = (MemorySegment) manifestFn.invokeExact(outLenSeg);
            try {
                long len = outLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                byte[] bytes = manifestPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
                manifest = FfmPluginProtocol.MAPPER.readValue(bytes, ManifestPayload.class);
            } finally {
                if (!MemorySegment.NULL.equals(manifestPtr)) {
                    freeFn.invokeExact(manifestPtr);
                }
            }
        }

        List<ManifestEntry> entries = (manifest != null && manifest.plugins() != null) ? manifest.plugins() : List.of();
        if (entries.isEmpty()) {
            throw new IllegalStateException("Plain C shared library exported an empty GatewayPlugin manifest: " + originalPath);
        }

        return new LoadedLibraryHandle(
            originalPath,
            stagedPath,
            mtime,
            AbiMode.PLAIN_C,
            libArena,
            entries,
            MemorySegment.NULL,
            null,
            null,
            null,
            null,
            invokeFn,
            freeFn,
            destroyFn
        );
    }

    private static final class LoadedLibraryHandle {
        final Path originalPath;
        final Path stagedPath;
        final long lastModifiedMillis;
        final long loadedAtMillis = System.currentTimeMillis();
        final AbiMode abiMode;
        final Arena libArena;
        final List<ManifestEntry> manifestEntries;

        final MemorySegment isolateAddr;
        final MethodHandle attachThreadFn;
        final MethodHandle detachThreadFn;
        final MethodHandle getCurrentThreadFn;
        final MethodHandle tearDownIsolateFn;
        final MethodHandle invokeFn;
        final MethodHandle freeFn;
        final MethodHandle plainDestroyFn;

        final List<String> registeredLookupKeys = new ArrayList<>();
        final Map<String, GatewayPlugin> adaptersByKey = new ConcurrentHashMap<>();
        final AtomicInteger refCount = new AtomicInteger(1);
        final AtomicBoolean retired = new AtomicBoolean(false);
        final AtomicBoolean destroyed = new AtomicBoolean(false);

        LoadedLibraryHandle(
            Path originalPath,
            Path stagedPath,
            long lastModifiedMillis,
            AbiMode abiMode,
            Arena libArena,
            List<ManifestEntry> manifestEntries,
            MemorySegment isolateAddr,
            MethodHandle attachThreadFn,
            MethodHandle detachThreadFn,
            MethodHandle getCurrentThreadFn,
            MethodHandle tearDownIsolateFn,
            MethodHandle invokeFn,
            MethodHandle freeFn,
            MethodHandle plainDestroyFn
        ) {
            this.originalPath = originalPath;
            this.stagedPath = stagedPath;
            this.lastModifiedMillis = lastModifiedMillis;
            this.abiMode = abiMode;
            this.libArena = libArena;
            this.manifestEntries = manifestEntries;
            this.isolateAddr = isolateAddr;
            this.attachThreadFn = attachThreadFn;
            this.detachThreadFn = detachThreadFn;
            this.getCurrentThreadFn = getCurrentThreadFn;
            this.tearDownIsolateFn = tearDownIsolateFn;
            this.invokeFn = invokeFn;
            this.freeFn = freeFn;
            this.plainDestroyFn = plainDestroyFn;
        }

        boolean tryAcquire() {
            while (true) {
                if (retired.get()) {
                    return false;
                }
                int cur = refCount.get();
                if (cur <= 0) {
                    return false;
                }
                if (refCount.compareAndSet(cur, cur + 1)) {
                    if (retired.get()) {
                        release();
                        return false;
                    }
                    return true;
                }
            }
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                destroyNativeResources();
            }
        }

        void retire() {
            if (retired.compareAndSet(false, true)) {
                release();
            }
        }

        private void destroyNativeResources() {
            if (!destroyed.compareAndSet(false, true)) {
                return;
            }
            try {
                if (abiMode == AbiMode.GRAAL_ISOLATE && !MemorySegment.NULL.equals(isolateAddr)) {
                    try (Arena tmp = Arena.ofConfined()) {
                        MemorySegment thr = MemorySegment.NULL;
                        if (getCurrentThreadFn != null) {
                            thr = (MemorySegment) getCurrentThreadFn.invokeExact(isolateAddr);
                        }
                        if (MemorySegment.NULL.equals(thr)) {
                            MemorySegment thrBox = tmp.allocate(ValueLayout.ADDRESS);
                            int rc = (int) attachThreadFn.invokeExact(isolateAddr, thrBox);
                            if (rc == 0) {
                                thr = thrBox.get(ValueLayout.ADDRESS, 0L);
                            }
                        }
                        if (!MemorySegment.NULL.equals(thr)) {
                            int unusedTearDownRc = (int) tearDownIsolateFn.invokeExact(thr);
                        }
                    }
                } else if (abiMode == AbiMode.PLAIN_C && plainDestroyFn != null) {
                    plainDestroyFn.invokeExact();
                }
            } catch (Throwable ignored) {
            } finally {
                try {
                    libArena.close();
                } catch (Throwable ignored) {}
                try {
                    Files.deleteIfExists(stagedPath);
                } catch (Throwable ignored) {}
            }
        }
    }

    public static final class FfmGatewayPluginAdapter implements GatewayPlugin {
        private final ManifestEntry entry;
        private final byte[] idBytes;
        private final LoadedLibraryHandle handle;
        private final MemorySegment upcallStub;

        FfmGatewayPluginAdapter(ManifestEntry entry, LoadedLibraryHandle handle, MemorySegment upcallStub) {
            this.entry = entry;
            this.idBytes = entry.id().getBytes(StandardCharsets.UTF_8);
            this.handle = handle;
            this.upcallStub = upcallStub;
        }

        @Override
        public String id() {
            return entry.id();
        }

        @Override
        public String name() {
            return entry.name() != null && !entry.name().isBlank() ? entry.name() : entry.id();
        }

        @Override
        public String type() {
            return "NATIVE_FFM";
        }

        @Override
        public int priority() {
            return entry.priority() > 0 ? entry.priority() : 100;
        }

        public String implementationClassName() {
            return entry.className() != null && !entry.className().isBlank()
                ? entry.className()
                : getClass().getName();
        }

        @Override
        public PluginResult executeStage(PluginStage stage, PluginExecutionContext ctx) {
            if (!handle.tryAcquire()) {
                throw new IllegalStateException("Native FFM plugin '" + id() + "' has been unloaded");
            }
            long callHandleId = CALL_HANDLE_SEQ.getAndIncrement();
            UPCALL_CONTEXTS.put(callHandleId, ctx);
            try (Arena callArena = Arena.ofConfined()) {
                byte[] ctxBytes = FfmPluginProtocol.MAPPER.writeValueAsBytes(ContextRequestEnvelope.fromContext(ctx));

                MemorySegment idSeg = callArena.allocate(Math.max(1, idBytes.length));
                if (idBytes.length > 0) {
                    MemorySegment.copy(idBytes, 0, idSeg, ValueLayout.JAVA_BYTE, 0L, idBytes.length);
                }

                MemorySegment reqSeg = callArena.allocate(Math.max(1, ctxBytes.length));
                if (ctxBytes.length > 0) {
                    MemorySegment.copy(ctxBytes, 0, reqSeg, ValueLayout.JAVA_BYTE, 0L, ctxBytes.length);
                }

                MemorySegment outLenSeg = callArena.allocate(ValueLayout.JAVA_LONG);
                outLenSeg.set(ValueLayout.JAVA_LONG, 0L, 0L);

                byte[] respBytes;
                if (handle.abiMode == AbiMode.GRAAL_ISOLATE) {
                    respBytes = invokeInGraalIsolate(callArena, idSeg, stage.ordinal(), reqSeg, ctxBytes.length, callHandleId, outLenSeg);
                } else {
                    respBytes = invokePlainC(idSeg, stage.ordinal(), reqSeg, ctxBytes.length, callHandleId, outLenSeg);
                }

                ContextResponseEnvelope respEnv = FfmPluginProtocol.MAPPER.readValue(respBytes, ContextResponseEnvelope.class);
                return respEnv.applyToAndBuildResult(ctx);
            } catch (RuntimeException re) {
                throw re;
            } catch (Throwable t) {
                throw new IllegalStateException("FFM invocation failed for plugin '" + id() + "': " + t.getMessage(), t);
            } finally {
                UPCALL_CONTEXTS.remove(callHandleId);
                handle.release();
            }
        }

        private byte[] invokeInGraalIsolate(
            Arena callArena,
            MemorySegment idSeg,
            int stageOrd,
            MemorySegment reqSeg,
            long reqLen,
            long callHandleId,
            MemorySegment outLenSeg
        ) throws Throwable {
            boolean attachedHere = false;
            MemorySegment isoThread = MemorySegment.NULL;
            if (handle.getCurrentThreadFn != null) {
                isoThread = (MemorySegment) handle.getCurrentThreadFn.invokeExact(handle.isolateAddr);
            }
            if (MemorySegment.NULL.equals(isoThread)) {
                MemorySegment thrBox = callArena.allocate(ValueLayout.ADDRESS);
                int rc = (int) handle.attachThreadFn.invokeExact(handle.isolateAddr, thrBox);
                if (rc != 0) {
                    throw new IllegalStateException("graal_attach_thread failed with code " + rc);
                }
                isoThread = thrBox.get(ValueLayout.ADDRESS, 0L);
                attachedHere = true;
            }
            try {
                MemorySegment respPtr = (MemorySegment) handle.invokeFn.invokeExact(
                    isoThread,
                    idSeg,
                    (long) idBytes.length,
                    stageOrd,
                    reqSeg,
                    reqLen,
                    upcallStub,
                    callHandleId,
                    outLenSeg
                );
                try {
                    long outLen = outLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                    if (MemorySegment.NULL.equals(respPtr) || outLen <= 0L) {
                        return "{\"action\":\"CONTINUE\"}".getBytes(StandardCharsets.UTF_8);
                    }
                    return respPtr.reinterpret(outLen).toArray(ValueLayout.JAVA_BYTE);
                } finally {
                    if (!MemorySegment.NULL.equals(respPtr)) {
                        handle.freeFn.invokeExact(isoThread, respPtr);
                    }
                }
            } finally {
                if (attachedHere && !MemorySegment.NULL.equals(isoThread)) {
                    int unusedDetachRc = (int) handle.detachThreadFn.invokeExact(isoThread);
                }
            }
        }

        private byte[] invokePlainC(
            MemorySegment idSeg,
            int stageOrd,
            MemorySegment reqSeg,
            long reqLen,
            long callHandleId,
            MemorySegment outLenSeg
        ) throws Throwable {
            MemorySegment respPtr = (MemorySegment) handle.invokeFn.invokeExact(
                idSeg,
                (long) idBytes.length,
                stageOrd,
                reqSeg,
                reqLen,
                upcallStub,
                callHandleId,
                outLenSeg
            );
            try {
                long outLen = outLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                if (MemorySegment.NULL.equals(respPtr) || outLen <= 0L) {
                    return "{\"action\":\"CONTINUE\"}".getBytes(StandardCharsets.UTF_8);
                }
                return respPtr.reinterpret(outLen).toArray(ValueLayout.JAVA_BYTE);
            } finally {
                if (!MemorySegment.NULL.equals(respPtr)) {
                    handle.freeFn.invokeExact(respPtr);
                }
            }
        }
    }
}
