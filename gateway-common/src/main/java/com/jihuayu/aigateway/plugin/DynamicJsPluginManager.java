package com.jihuayu.aigateway.plugin;

import com.jihuayu.aigateway.hot.HotContract.PluginSnapshot;
import com.jihuayu.aigateway.plugin.JsPluginSandbox.CompiledJsPlugin;
import com.jihuayu.aigateway.security.SecurityUtils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Dual-Channel Dynamic Runtime JS Plugin Manager (#41).
 *
 * <p>Supports runtime dynamic loading and hot-swapping of {@code .js} plugins without gateway restart:
 * <ul>
 *   <li><b>Channel A (Control Plane / Redis Snapshot Hot Push)</b>: Compiles and caches {@link CompiledJsPlugin}
 *       instances keyed by {@code (pluginId, codeSha256)} with Copy-on-Write atomic replacement.</li>
 *   <li><b>Channel B (Local {@code PLUGINS_DIR/*.js} & {@code *.jar} Directory Hot Watcher)</b>:
 *       Monitors a local plugin directory for newly added, modified, or deleted {@code .js} (and {@code .jar})
 *       files and atomically hot-swaps them in memory.</li>
 * </ul>
 */
public class DynamicJsPluginManager implements AutoCloseable {

    private final Map<String, CompiledJsPlugin> snapshotJsPlugins = new ConcurrentHashMap<>();
    private final Map<String, CompiledJsPlugin> directoryJsPlugins = new ConcurrentHashMap<>();
    private final Map<Path, Long> directoryModifiedTimes = new ConcurrentHashMap<>();
    private final DynamicJarPluginLoader jarLoader;

    private volatile Path watchedDirectory;
    private volatile boolean watcherRunning = false;
    private volatile Thread watcherThread;

    public DynamicJsPluginManager(DynamicJarPluginLoader jarLoader) {
        this.jarLoader = jarLoader;
    }

    /**
     * Channel A: Retrieves or compiles a JS plugin from a Redis/Control-Plane {@link PluginSnapshot}
     * using Copy-on-Write atomic replacement when {@code codeSha256} changes.
     */
    public CompiledJsPlugin getOrCompileSnapshotPlugin(PluginSnapshot snap) {
        if (snap == null || snap.sourceCode() == null || snap.sourceCode().isBlank()) {
            return null;
        }
        String sha = (snap.codeSha256() != null && !snap.codeSha256().isBlank())
            ? snap.codeSha256()
            : SecurityUtils.hashKey(snap.sourceCode());
        CompiledJsPlugin existing = snapshotJsPlugins.get(snap.id());
        if (existing != null && sha.equals(existing.codeSha256())) {
            return existing;
        }
        CompiledJsPlugin compiled = JsPluginSandbox.compile(
            snap.id(),
            snap.name(),
            sha,
            snap.sourceCode(),
            snap.priority(),
            snap.maxStatements() > 0 ? snap.maxStatements() : 10_000
        );
        snapshotJsPlugins.put(snap.id(), compiled);
        return compiled;
    }

    /**
     * Directly registers or hot-swaps a JS plugin from source code at runtime.
     */
    public CompiledJsPlugin registerOrUpdateJs(
        String id,
        String name,
        String sourceCode,
        int priority,
        int maxStatements
    ) {
        String sha = SecurityUtils.hashKey(sourceCode);
        CompiledJsPlugin compiled = JsPluginSandbox.compile(id, name, sha, sourceCode, priority, maxStatements);
        snapshotJsPlugins.put(id, compiled);
        return compiled;
    }

    /**
     * Channel B: Scans a local directory for {@code *.js} (and {@code *.jar} in JVM mode) plugins
     * and atomically hot-reloads any created, modified, or deleted files.
     */
    public synchronized List<String> scanDirectoryOnce(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) {
            return List.of();
        }
        List<String> activeIds = new ArrayList<>();
        Set<Path> seenPaths = ConcurrentHashMap.newKeySet();
        try (Stream<Path> stream = Files.list(dir)) {
            for (Path file : stream.toList()) {
                if (!Files.isRegularFile(file)) continue;
                Path normalized = file.toAbsolutePath().normalize();
                String fileName = normalized.getFileName().toString();
                if (fileName.endsWith(".js")) {
                    seenPaths.add(normalized);
                    long mtime = Files.getLastModifiedTime(normalized).toMillis();
                    Long prevMtime = directoryModifiedTimes.get(normalized);
                    String pluginId = "dir-js:" + fileName.substring(0, fileName.length() - 3);
                    if (prevMtime == null || prevMtime != mtime || !directoryJsPlugins.containsKey(pluginId)) {
                        String code = Files.readString(normalized, StandardCharsets.UTF_8);
                        String sha = SecurityUtils.hashKey(code);
                        CompiledJsPlugin compiled = JsPluginSandbox.compile(
                            pluginId,
                            fileName,
                            sha,
                            code,
                            50,
                            10_000
                        );
                        directoryJsPlugins.put(pluginId, compiled);
                        directoryModifiedTimes.put(normalized, mtime);
                    }
                    activeIds.add(pluginId);
                } else if (fileName.endsWith(".jar") && jarLoader != null && !DynamicJarPluginLoader.isNativeImage()) {
                    seenPaths.add(normalized);
                    long mtime = Files.getLastModifiedTime(normalized).toMillis();
                    if (jarLoader.isJarModified(normalized, mtime)) {
                        List<GatewayPlugin> loaded = jarLoader.loadJar(normalized);
                        for (GatewayPlugin gp : loaded) {
                            activeIds.add(gp.id());
                        }
                        directoryModifiedTimes.put(normalized, mtime);
                    }
                }
            }
        } catch (Exception ignored) {}

        // Remove any deleted .js or .jar files from active maps
        for (Path prevPath : new ArrayList<>(directoryModifiedTimes.keySet())) {
            if (!seenPaths.contains(prevPath)) {
                directoryModifiedTimes.remove(prevPath);
                String fn = prevPath.getFileName().toString();
                if (fn.endsWith(".js")) {
                    String pid = "dir-js:" + fn.substring(0, fn.length() - 3);
                    directoryJsPlugins.remove(pid);
                } else if (fn.endsWith(".jar") && jarLoader != null) {
                    jarLoader.unloadJar(prevPath);
                }
            }
        }
        return activeIds;
    }

    /**
     * Starts a background virtual thread that continuously watches {@code pluginsDir} for hot changes.
     */
    public synchronized void startDirectoryWatcher(Path pluginsDir, long pollIntervalMs) {
        stopDirectoryWatcher();
        if (pluginsDir == null) return;
        try {
            Files.createDirectories(pluginsDir);
        } catch (Exception ignored) {}
        this.watchedDirectory = pluginsDir.toAbsolutePath().normalize();
        scanDirectoryOnce(this.watchedDirectory);
        this.watcherRunning = true;
        long sleepMs = Math.max(50L, pollIntervalMs);
        this.watcherThread = Thread.ofVirtual().name("js-plugin-dir-watcher").start(() -> {
            while (watcherRunning) {
                try {
                    scanDirectoryOnce(this.watchedDirectory);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {}
            }
        });
    }

    public synchronized void stopDirectoryWatcher() {
        this.watcherRunning = false;
        if (this.watcherThread != null) {
            this.watcherThread.interrupt();
            this.watcherThread = null;
        }
    }

    public Map<String, CompiledJsPlugin> activeDirectoryJsPlugins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(directoryJsPlugins));
    }

    public Map<String, CompiledJsPlugin> activeSnapshotJsPlugins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(snapshotJsPlugins));
    }

    @Override
    public void close() {
        stopDirectoryWatcher();
    }
}
