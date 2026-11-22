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
 * Isomorphic Java SPI Plugin Loader (#36).
 *
 * <p>Supports:
 * <ol>
 *   <li>Compile-time discovery via standard {@link ServiceLoader} (works in both JVM and GraalVM Native Image).</li>
 *   <li>Runtime dynamic {@code .jar} hot-loading via isolated {@link URLClassLoader} when running in JVM mode.</li>
 * </ol>
 */
public class DynamicJarPluginLoader {

    private record LoadedJarHandle(
        Path jarPath,
        long lastModifiedMillis,
        URLClassLoader classLoader,
        List<String> pluginIds
    ) {}

    private final Map<String, GatewayPlugin> activePlugins = new ConcurrentHashMap<>();
    private final Map<Path, LoadedJarHandle> loadedJars = new ConcurrentHashMap<>();

    public DynamicJarPluginLoader() {
        reloadCompileTimeProviders();
    }

    public static boolean isNativeImage() {
        return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
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

    public void registerPlugin(GatewayPlugin plugin) {
        if (plugin != null && plugin.id() != null && !plugin.id().isBlank()) {
            activePlugins.put(plugin.id(), plugin);
            if (!plugin.name().equals(plugin.id())) {
                activePlugins.put(plugin.name(), plugin);
            }
        }
    }

    /**
     * Dynamically loads all {@link GatewayPlugin} SPI providers from an external {@code .jar} file in JVM mode.
     */
    public synchronized List<GatewayPlugin> loadJar(Path jarPath) {
        if (isNativeImage()) {
            throw new UnsupportedOperationException(
                "Dynamic .jar classloading is disabled in GraalVM Native Image closed-world mode; use compile-time SPI or dynamic .js plugins."
            );
        }
        if (jarPath == null || !Files.isRegularFile(jarPath)) {
            throw new IllegalArgumentException("Plugin JAR file does not exist: " + jarPath);
        }
        Path normalized = jarPath.toAbsolutePath().normalize();
        unloadJar(normalized);
        try {
            long mtime = Files.getLastModifiedTime(normalized).toMillis();
            URL jarUrl = normalized.toUri().toURL();
            URLClassLoader cl = new URLClassLoader(new URL[]{jarUrl}, GatewayPlugin.class.getClassLoader());
            ServiceLoader<GatewayPlugin> loader = ServiceLoader.load(GatewayPlugin.class, cl);
            List<GatewayPlugin> loaded = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (GatewayPlugin plugin : loader) {
                activePlugins.put(plugin.id(), plugin);
                activePlugins.put(plugin.getClass().getName(), plugin);
                loaded.add(plugin);
                ids.add(plugin.id());
            }
            loadedJars.put(normalized, new LoadedJarHandle(normalized, mtime, cl, ids));
            return loaded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load plugin JAR " + normalized + ": " + e.getMessage(), e);
        }
    }

    /**
     * Unloads a previously dynamically loaded {@code .jar} plugin and closes its isolated {@link URLClassLoader}.
     */
    public synchronized boolean unloadJar(Path jarPath) {
        if (jarPath == null) return false;
        Path normalized = jarPath.toAbsolutePath().normalize();
        LoadedJarHandle handle = loadedJars.remove(normalized);
        if (handle == null) return false;
        for (String id : handle.pluginIds()) {
            activePlugins.remove(id);
        }
        try {
            handle.classLoader().close();
        } catch (Exception ignored) {}
        return true;
    }

    public GatewayPlugin getPlugin(String idOrClassName) {
        if (idOrClassName == null) return null;
        return activePlugins.get(idOrClassName);
    }

    public Map<String, GatewayPlugin> allPlugins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(activePlugins));
    }

    public boolean isJarModified(Path jarPath, long currentMtime) {
        if (jarPath == null) return false;
        LoadedJarHandle handle = loadedJars.get(jarPath.toAbsolutePath().normalize());
        return handle == null || handle.lastModifiedMillis() != currentMtime;
    }
}
