package com.jihuayu.aigateway.plugin;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Isomorphic Java SPI &amp; Native FFM Shared-Library Plugin Loader (#36).
 *
 * <p>Supports:
 * <ol>
 *   <li>Compile-time discovery via standard {@link ServiceLoader} (works in both JVM and GraalVM Native Image).</li>
 *   <li>Runtime dynamic {@code .jar} hot-loading via isolated {@link URLClassLoader} when running in JVM mode.</li>
 *   <li>Runtime dynamic {@code .so}/{@code .dylib}/{@code .dll} hot-loading &amp; deterministic Isolate unloading via
 *       {@link NativeFfmPluginLoader} in both JVM and GraalVM Native Image modes — including unmodified {@code .jar}
 *       plugins wrapped with {@link JarToSharedLibCompiler} ({@code native-image --shared}).</li>
 * </ol>
 */
public class DynamicJarPluginLoader implements AutoCloseable {

    private record LoadedJarHandle(
        Path jarPath,
        long lastModifiedMillis,
        URLClassLoader classLoader,
        List<String> pluginIds,
        Map<String, GatewayPlugin> pluginsByKey
    ) {}

    private final Map<String, GatewayPlugin> baselinePlugins = new ConcurrentHashMap<>();
    private final Map<String, GatewayPlugin> activePlugins = new ConcurrentHashMap<>();
    private final Map<Path, LoadedJarHandle> loadedJars = new ConcurrentHashMap<>();
    private final Map<Path, Path> compiledOrCompanionLibByJar = new ConcurrentHashMap<>();
    private final NativeFfmPluginLoader ffmLoader;

    public DynamicJarPluginLoader() {
        this.ffmLoader = new NativeFfmPluginLoader(
            activePlugins::put,
            this::restoreBaselineOrRemoveKey
        );
        reloadCompileTimeProviders();
    }

    public static boolean isNativeImage() {
        return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    private void restoreBaselineOrRemoveKey(String key) {
        if (key == null) return;
        GatewayPlugin baseline = baselinePlugins.get(key);
        if (baseline != null) {
            activePlugins.put(key, baseline);
        } else {
            activePlugins.remove(key);
        }
    }

    /**
     * Discovers all compile-time {@link GatewayPlugin} implementations on the application classpath.
     */
    public final List<String> reloadCompileTimeProviders() {
        List<String> discovered = new ArrayList<>();
        ServiceLoader<GatewayPlugin> loader = ServiceLoader.load(GatewayPlugin.class, GatewayPlugin.class.getClassLoader());
        for (GatewayPlugin plugin : loader) {
            registerPlugin(plugin);
            discovered.add(plugin.id());
        }
        return discovered;
    }

    public void registerAlias(String alias, GatewayPlugin plugin) {
        if (alias != null && !alias.isBlank() && plugin != null) {
            baselinePlugins.put(alias, plugin);
            activePlugins.put(alias, plugin);
        }
    }

    public void registerPlugin(GatewayPlugin plugin) {
        if (plugin != null && plugin.id() != null && !plugin.id().isBlank()) {
            baselinePlugins.put(plugin.id(), plugin);
            activePlugins.put(plugin.id(), plugin);
            if (!plugin.name().equals(plugin.id())) {
                baselinePlugins.put(plugin.name(), plugin);
                activePlugins.put(plugin.name(), plugin);
            }
            String clsName = plugin.getClass().getName();
            baselinePlugins.put(clsName, plugin);
            activePlugins.put(clsName, plugin);
        }
    }

    public NativeFfmPluginLoader nativeFfmLoader() {
        return ffmLoader;
    }

    /**
     * Dynamically loads (or hot-swaps) a native shared library ({@code .so}/{@code .dylib}/{@code .dll}) via JDK 25 FFM.
     */
    public synchronized List<GatewayPlugin> loadNativeLibrary(Path libPath) {
        return ffmLoader.loadSharedLibrary(libPath);
    }

    /**
     * Unloads a dynamically mounted native shared library, draining in-flight calls and tearing down its GraalVM Isolate.
     */
    public synchronized boolean unloadNativeLibrary(Path libPath) {
        if (libPath == null) return false;
        Path normalized = libPath.toAbsolutePath().normalize();
        compiledOrCompanionLibByJar.values().removeIf(p -> p.equals(normalized));
        return ffmLoader.unloadSharedLibrary(normalized);
    }

    /**
     * Compiles an unmodified {@link GatewayPlugin} {@code .jar} into a GraalVM {@code --shared} library
     * and dynamically loads it via FFM.
     */
    public synchronized List<GatewayPlugin> compileAndLoadJarAsNative(Path jarPath, Path outputLibPath) {
        if (jarPath == null || !Files.isRegularFile(jarPath)) {
            throw new IllegalArgumentException("Plugin JAR file does not exist: " + jarPath);
        }
        Path normalizedJar = jarPath.toAbsolutePath().normalize();
        Path compiledLib = JarToSharedLibCompiler.compileJarToSharedLib(normalizedJar, outputLibPath);
        List<GatewayPlugin> loaded = ffmLoader.loadSharedLibrary(compiledLib);
        compiledOrCompanionLibByJar.put(normalizedJar, compiledLib);
        return loaded;
    }

    /**
     * Resolves a companion {@code .so}/{@code .dylib}/{@code .dll} placed alongside a {@code .jar} file, if present.
     */
    public Path findCompanionSharedLibrary(Path jarPath) {
        if (jarPath == null) return null;
        Path normalized = jarPath.toAbsolutePath().normalize();
        Path parent = normalized.getParent();
        if (parent == null) return null;
        String fileName = normalized.getFileName().toString();
        String stem = fileName.endsWith(".jar") ? fileName.substring(0, fileName.length() - 4) : fileName;
        String safeStem = stem.replaceAll("[^A-Za-z0-9_-]", "_");
        String ext = NativeFfmPluginLoader.sharedLibraryExtension();
        for (String candidateName : List.of(
            "lib" + stem + ext,
            "lib" + safeStem + ext,
            stem + ext,
            safeStem + ext,
            fileName + ext
        )) {
            Path candidate = parent.resolve(candidateName);
            if (Files.isRegularFile(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
        }
        return null;
    }

    /**
     * Dynamically loads all {@link GatewayPlugin} SPI providers from an external {@code .jar} (or {@code .so}/{@code .dylib}/{@code .dll}).
     *
     * <p>In GraalVM Native Image mode, automatically resolves any companion {@code --shared} native library
     * alongside the {@code .jar} (or compiles on-the-fly if {@code native-image} is available on the host).
     */
    public synchronized List<GatewayPlugin> loadJar(Path jarPath) {
        if (jarPath == null || !Files.isRegularFile(jarPath)) {
            throw new IllegalArgumentException("Plugin file does not exist: " + jarPath);
        }
        Path normalized = jarPath.toAbsolutePath().normalize();
        String fileName = normalized.getFileName().toString();
        if (NativeFfmPluginLoader.isSharedLibraryFileName(fileName)) {
            return loadNativeLibrary(normalized);
        }
        if (isNativeImage()) {
            Path companion = findCompanionSharedLibrary(normalized);
            if (companion != null) {
                List<GatewayPlugin> loaded = ffmLoader.loadSharedLibrary(companion);
                compiledOrCompanionLibByJar.put(normalized, companion);
                return loaded;
            }
            if (JarToSharedLibCompiler.isNativeImageToolAvailable()) {
                try {
                    Path tempOutDir = Files.createTempDirectory("ai-gw-auto-shared-");
                    String stem = fileName.endsWith(".jar") ? fileName.substring(0, fileName.length() - 4) : fileName;
                    Path targetSo = tempOutDir.resolve("lib" + stem.replaceAll("[^A-Za-z0-9_-]", "_")
                        + NativeFfmPluginLoader.sharedLibraryExtension());
                    return compileAndLoadJarAsNative(normalized, targetSo);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Failed to auto-compile JAR plugin for Native Image FFM loading: " + e.getMessage(), e
                    );
                }
            }
            throw new UnsupportedOperationException(
                "Direct bytecode .jar classloading is disabled in GraalVM Native Image mode; "
                    + "compile the unmodified .jar via './gradlew :gateway-common:compilePluginSharedLib -PpluginJar="
                    + normalized + "' and load the resulting " + NativeFfmPluginLoader.sharedLibraryExtension()
                    + " shared library (or place it next to the .jar)."
            );
        }

        unloadJar(normalized);
        try {
            long mtime = Files.getLastModifiedTime(normalized).toMillis();
            URL jarUrl = normalized.toUri().toURL();
            URLClassLoader cl = new URLClassLoader(new URL[]{jarUrl}, GatewayPlugin.class.getClassLoader());
            ServiceLoader<GatewayPlugin> loader = ServiceLoader.load(GatewayPlugin.class, cl);
            List<GatewayPlugin> loaded = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            Map<String, GatewayPlugin> registeredByKey = new LinkedHashMap<>();
            for (GatewayPlugin plugin : loader) {
                activePlugins.put(plugin.id(), plugin);
                registeredByKey.put(plugin.id(), plugin);
                if (!plugin.name().equals(plugin.id())) {
                    activePlugins.put(plugin.name(), plugin);
                    registeredByKey.put(plugin.name(), plugin);
                }
                String clsName = plugin.getClass().getName();
                activePlugins.put(clsName, plugin);
                registeredByKey.put(clsName, plugin);
                loaded.add(plugin);
                ids.add(plugin.id());
            }
            loadedJars.put(normalized, new LoadedJarHandle(normalized, mtime, cl, ids, registeredByKey));
            return loaded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load plugin JAR " + normalized + ": " + e.getMessage(), e);
        }
    }

    /**
     * Unloads a previously dynamically loaded {@code .jar} (or native {@code .so}/{@code .dylib}/{@code .dll}) plugin.
     */
    public synchronized boolean unloadJar(Path jarPath) {
        if (jarPath == null) return false;
        Path normalized = jarPath.toAbsolutePath().normalize();
        if (NativeFfmPluginLoader.isSharedLibraryFileName(normalized.getFileName().toString())) {
            return unloadNativeLibrary(normalized);
        }
        boolean unloadedNative = false;
        Path mappedNative = compiledOrCompanionLibByJar.remove(normalized);
        if (mappedNative != null) {
            unloadedNative = ffmLoader.unloadSharedLibrary(mappedNative);
        }
        LoadedJarHandle handle = loadedJars.remove(normalized);
        if (handle == null) {
            return unloadedNative;
        }
        for (Map.Entry<String, GatewayPlugin> entry : handle.pluginsByKey().entrySet()) {
            String key = entry.getKey();
            if (activePlugins.remove(key, entry.getValue())) {
                GatewayPlugin baseline = baselinePlugins.get(key);
                if (baseline != null) {
                    activePlugins.put(key, baseline);
                }
            }
        }
        try {
            handle.classLoader().close();
        } catch (Exception ignored) {}
        return true;
    }

    /**
     * Unloads a dynamically loaded native library or JAR owning the specified plugin ID, name, or class name.
     */
    public synchronized boolean unloadPluginById(String idOrClassName) {
        if (idOrClassName == null || idOrClassName.isBlank()) return false;
        String target = idOrClassName.trim();
        if (ffmLoader.unloadPluginById(target)) {
            return true;
        }
        for (LoadedJarHandle handle : new ArrayList<>(loadedJars.values())) {
            if (handle.pluginsByKey().containsKey(target)) {
                return unloadJar(handle.jarPath());
            }
        }
        return false;
    }

    public GatewayPlugin getPlugin(String idOrClassName) {
        if (idOrClassName == null) return null;
        GatewayPlugin found = activePlugins.get(idOrClassName);
        if (found != null) return found;
        return ffmLoader.getPlugin(idOrClassName);
    }

    public Map<String, GatewayPlugin> allPlugins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(activePlugins));
    }

    public boolean isJarModified(Path jarPath, long currentMtime) {
        if (jarPath == null) return false;
        Path normalized = jarPath.toAbsolutePath().normalize();
        if (NativeFfmPluginLoader.isSharedLibraryFileName(normalized.getFileName().toString())) {
            return ffmLoader.isLibraryModified(normalized, currentMtime);
        }
        Path mappedNative = compiledOrCompanionLibByJar.get(normalized);
        if (mappedNative != null) {
            return ffmLoader.isLibraryModified(mappedNative, currentMtime);
        }
        LoadedJarHandle handle = loadedJars.get(normalized);
        return handle == null || handle.lastModifiedMillis() != currentMtime;
    }

    public boolean isNativeLibraryModified(Path libPath, long currentMtime) {
        return ffmLoader.isLibraryModified(libPath, currentMtime);
    }

    @Override
    public synchronized void close() {
        ffmLoader.close();
        for (Path jarPath : new ArrayList<>(loadedJars.keySet())) {
            unloadJar(jarPath);
        }
    }
}
