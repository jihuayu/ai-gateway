package com.jihuayu.aigateway.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Zero-Modification {@code GatewayPlugin} {@code .jar} to GraalVM {@code --shared} Native Library Compiler (#36).
 *
 * <p>Wraps any standard {@link GatewayPlugin} {@code .jar} (containing
 * {@code META-INF/services/com.jihuayu.aigateway.plugin.GatewayPlugin}) with {@link SharedLibPluginBridge},
 * automatically generates reflection metadata for all classes inside the {@code .jar}, and invokes
 * {@code native-image --shared} to emit a loadable {@code .so}/{@code .dylib}/{@code .dll} artifact.
 */
public final class JarToSharedLibCompiler {

    private static final String SPI_ENTRY = "META-INF/services/com.jihuayu.aigateway.plugin.GatewayPlugin";

    private JarToSharedLibCompiler() {}

    public record JarInspection(
        List<String> providerClassNames,
        List<String> allJarClassNames
    ) {}

    /**
     * Inspects an unmodified {@code .jar} plugin to extract declared {@link GatewayPlugin} SPI providers
     * and all packaged classes for automatic GraalVM reflection registration.
     */
    public static JarInspection inspectPluginJar(Path jarPath) {
        if (jarPath == null || !Files.isRegularFile(jarPath)) {
            throw new IllegalArgumentException("Plugin JAR file does not exist: " + jarPath);
        }
        Set<String> providers = new LinkedHashSet<>();
        Set<String> classes = new LinkedHashSet<>();
        try (JarFile jf = new JarFile(jarPath.toFile())) {
            JarEntry spi = jf.getJarEntry(SPI_ENTRY);
            if (spi != null) {
                try (InputStream in = jf.getInputStream(spi);
                     BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        int hash = line.indexOf('#');
                        String clean = (hash >= 0 ? line.substring(0, hash) : line).trim();
                        if (!clean.isEmpty()) {
                            providers.add(clean);
                        }
                    }
                }
            }
            Enumeration<JarEntry> entries = jf.entries();
            while (entries.hasMoreElements()) {
                JarEntry je = entries.nextElement();
                String name = je.getName();
                if (!je.isDirectory() && name.endsWith(".class") && !name.equals("module-info.class")
                    && !name.startsWith("META-INF/")) {
                    classes.add(name.substring(0, name.length() - 6).replace('/', '.'));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to inspect plugin JAR " + jarPath + ": " + e.getMessage(), e);
        }
        if (providers.isEmpty()) {
            throw new IllegalArgumentException(
                "JAR '" + jarPath + "' does not declare any providers in " + SPI_ENTRY
            );
        }
        classes.addAll(providers);
        return new JarInspection(List.copyOf(providers), List.copyOf(classes));
    }

    /**
     * Locates the {@code native-image} binary from {@code GRAALVM_HOME}, {@code JAVA_HOME}, or {@code PATH}.
     */
    public static Path findNativeImageExecutable() {
        boolean isWin = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        String exeName = isWin ? "native-image.cmd" : "native-image";

        for (String envKey : List.of("GRAALVM_HOME", "JAVA_HOME")) {
            String dir = System.getenv(envKey);
            if (dir != null && !dir.isBlank()) {
                Path candidate = Path.of(dir.trim(), "bin", exeName);
                if (Files.isExecutable(candidate)) {
                    return candidate.toAbsolutePath().normalize();
                }
            }
        }
        String javaHomeProp = System.getProperty("java.home");
        if (javaHomeProp != null && !javaHomeProp.isBlank()) {
            Path candidate = Path.of(javaHomeProp.trim(), "bin", exeName);
            if (Files.isExecutable(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
        }
        String pathEnv = System.getenv("PATH");
        if (pathEnv != null) {
            for (String part : pathEnv.split(File.pathSeparator)) {
                if (part == null || part.isBlank()) continue;
                Path candidate = Path.of(part.trim(), exeName);
                if (Files.isExecutable(candidate)) {
                    return candidate.toAbsolutePath().normalize();
                }
            }
        }
        return null;
    }

    public static boolean isNativeImageToolAvailable() {
        return findNativeImageExecutable() != null;
    }

    /**
     * Compiles an unmodified {@link GatewayPlugin} {@code .jar} into a {@code --shared} native library
     * ({@code .so} / {@code .dylib} / {@code .dll}) ready to be hot-loaded by {@link NativeFfmPluginLoader}.
     */
    public static Path compileJarToSharedLib(Path jarPath, Path outputSharedLibPath) {
        return compileJarToSharedLib(jarPath, outputSharedLibPath, resolveBridgeClasspath());
    }

    public static Path compileJarToSharedLib(Path jarPath, Path outputSharedLibPath, String bridgeClasspath) {
        Path normalizedJar = jarPath.toAbsolutePath().normalize();
        JarInspection inspection = inspectPluginJar(normalizedJar);

        Path nativeImageExe = findNativeImageExecutable();
        if (nativeImageExe == null) {
            throw new IllegalStateException(
                "GraalVM 'native-image' executable not found in GRAALVM_HOME, JAVA_HOME, or PATH"
            );
        }
        if (bridgeClasspath == null || bridgeClasspath.isBlank()) {
            throw new IllegalStateException(
                "Bridge classpath is empty; set AI_GATEWAY_SHARED_BUILD_CP or run from JVM build/control-plane classpath"
            );
        }

        String ext = NativeFfmPluginLoader.sharedLibraryExtension();
        Path targetLibPath;
        if (outputSharedLibPath != null) {
            String raw = outputSharedLibPath.toAbsolutePath().normalize().toString();
            targetLibPath = raw.endsWith(ext) ? Path.of(raw) : Path.of(raw + ext);
        } else {
            String jarName = normalizedJar.getFileName().toString();
            String stem = jarName.endsWith(".jar") ? jarName.substring(0, jarName.length() - 4) : jarName;
            String libPrefix = ".dll".equals(ext) ? "" : "lib";
            targetLibPath = normalizedJar.getParent().resolve(libPrefix + stem.replaceAll("[^A-Za-z0-9_-]", "_") + ext);
        }

        String targetFileName = targetLibPath.getFileName().toString();
        String outputBaseName = targetFileName.endsWith(ext)
            ? targetFileName.substring(0, targetFileName.length() - ext.length())
            : targetFileName;

        Path tempWorkDir = null;
        try {
            Path parentDir = targetLibPath.getParent() != null
                ? targetLibPath.getParent()
                : Path.of(".").toAbsolutePath().normalize();
            Files.createDirectories(parentDir);
            tempWorkDir = Files.createDirectories(parentDir.resolve(".ai-gw-shared-build-" + UUID.randomUUID()));
            Path reflectFile = tempWorkDir.resolve("plugin-reflect-config.json");
            List<Map<String, Object>> reflectEntries = new ArrayList<>();
            for (String cn : inspection.allJarClassNames()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("name", cn);
                item.put("allDeclaredConstructors", true);
                item.put("allDeclaredMethods", true);
                item.put("allDeclaredFields", true);
                reflectEntries.add(item);
            }
            Files.writeString(
                reflectFile,
                FfmPluginProtocol.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(reflectEntries),
                StandardCharsets.UTF_8
            );

            Path outBase = parentDir.resolve(outputBaseName);
            String fullCp = bridgeClasspath + File.pathSeparator + normalizedJar;
            String targetCsv = String.join(",", inspection.providerClassNames());

            // Note: -J-D... sets hosted Native Image builder VM properties (required for CEntryPoint.include
            // and --initialize-at-build-time), while -D... sets runtime Isolate properties.
            List<String> cmd = List.of(
                nativeImageExe.toString(),
                "--shared",
                "-Ob",
                "--enable-preview",
                "--no-fallback",
                "--initialize-at-build-time=com.jihuayu.aigateway.plugin.SharedLibPluginBridge$BuildTimeTargetConfig",
                "-H:+UnlockExperimentalVMOptions",
                "-H:+ReportExceptionStackTraces",
                "-H:TempDirectory=" + tempWorkDir,
                "-J-Dai.gateway.shared.plugin.build=true",
                "-Dai.gateway.shared.plugin.build=true",
                "-J-Dai.gateway.shared.plugin.targetClasses=" + targetCsv,
                "-Dai.gateway.shared.plugin.targetClasses=" + targetCsv,
                "-H:ReflectionConfigurationFiles=" + reflectFile,
                "-cp",
                fullCp,
                "-o",
                outBase.toString()
            );

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.directory(tempWorkDir.toFile());
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            String buildOutput;
            try (InputStream in = proc.getInputStream()) {
                buildOutput = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            int code = proc.waitFor();
            if (code != 0 || !Files.isRegularFile(targetLibPath)) {
                throw new IllegalStateException(
                    "native-image --shared failed (exit " + code + ") for " + normalizedJar + ":\n" + buildOutput
                );
            }
            for (String hdrSuffix : List.of(".h", "_dynamic.h")) {
                Files.deleteIfExists(parentDir.resolve(outputBaseName + hdrSuffix));
            }
            Files.deleteIfExists(parentDir.resolve("graal_isolate.h"));
            Files.deleteIfExists(parentDir.resolve("graal_isolate_dynamic.h"));

            return targetLibPath;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compile JAR to shared library: " + e.getMessage(), e);
        } finally {
            if (tempWorkDir != null) {
                try (var walk = Files.walk(tempWorkDir)) {
                    walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                    });
                } catch (Exception ignored) {}
            }
        }
    }

    public static String resolveBridgeClasspath() {
        String envCp = System.getenv("AI_GATEWAY_SHARED_BUILD_CP");
        if (envCp != null && !envCp.isBlank()) {
            return envCp.trim();
        }
        boolean includeFull = Boolean.getBoolean("ai.gateway.shared.plugin.fullClasspath");
        Set<String> entries = new LinkedHashSet<>();

        for (Class<?> anchor : List.of(
            SharedLibPluginBridge.class,
            com.fasterxml.jackson.databind.ObjectMapper.class,
            com.fasterxml.jackson.core.JsonParser.class,
            com.fasterxml.jackson.annotation.JsonProperty.class,
            com.fasterxml.jackson.datatype.jsr310.JavaTimeModule.class,
            org.graalvm.nativeimage.c.function.CEntryPoint.class,
            org.graalvm.word.WordFactory.class,
            io.lettuce.core.api.StatefulRedisConnection.class
        )) {
            try {
                CodeSource cs = anchor.getProtectionDomain().getCodeSource();
                if (cs != null && cs.getLocation() != null) {
                    addUrlToClasspathSet(entries, cs.getLocation(), true);
                }
            } catch (Exception ignored) {}
        }

        ClassLoader cl = SharedLibPluginBridge.class.getClassLoader();
        while (cl != null) {
            if (cl instanceof URLClassLoader ucl) {
                for (URL url : ucl.getURLs()) {
                    addUrlToClasspathSet(entries, url, includeFull);
                }
            }
            cl = cl.getParent();
        }

        String sysCp = System.getProperty("java.class.path");
        if (sysCp != null && !sysCp.isBlank()) {
            for (String part : sysCp.split(File.pathSeparator)) {
                if (part != null && !part.isBlank()) {
                    Path p = Path.of(part.trim()).toAbsolutePath().normalize();
                    if (Files.exists(p) && shouldIncludeClasspathEntry(p, includeFull)) {
                        entries.add(p.toString());
                    }
                }
            }
        }
        return String.join(File.pathSeparator, entries);
    }

    private static void addUrlToClasspathSet(Set<String> out, URL url, boolean includeFull) {
        if (url == null || !"file".equalsIgnoreCase(url.getProtocol())) return;
        try {
            Path p = Path.of(url.toURI()).toAbsolutePath().normalize();
            if (Files.exists(p) && shouldIncludeClasspathEntry(p, includeFull)) {
                out.add(p.toString());
            }
        } catch (Exception ignored) {}
    }

    private static boolean shouldIncludeClasspathEntry(Path path, boolean includeFull) {
        String fileName = path.getFileName() != null ? path.getFileName().toString().toLowerCase(Locale.ROOT) : "";
        String full = path.toString().replace('\\', '/').toLowerCase(Locale.ROOT);
        if (fileName.contains("gradle-worker")
            || fileName.startsWith("junit-")
            || fileName.startsWith("opentest4j-")
            || fileName.startsWith("apiguardian-")
            || full.contains("/classes/java/test")
            || full.contains("/resources/test")) {
            return false;
        }
        if (!includeFull) {
            if (fileName.startsWith("spring-")
                || fileName.startsWith("bcprov-")
                || fileName.startsWith("gson-")
                || fileName.startsWith("testcontainers-")
                || fileName.startsWith("postgresql-")
                || fileName.startsWith("flyway-")
                || fileName.startsWith("jooq-")) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Usage: JarToSharedLibCompiler <input-plugin.jar> [output-lib-path]");
            System.exit(1);
        }
        Path inJar = Path.of(args[0]);
        Path outLib = args.length == 2 ? Path.of(args[1]) : null;
        long t0 = System.nanoTime();
        Path built = compileJarToSharedLib(inJar, outLib);
        double sec = (System.nanoTime() - t0) / 1_000_000_000.0;
        System.out.printf("Compiled %s -> %s in %.2f s%n", inJar.toAbsolutePath(), built.toAbsolutePath(), sec);
    }
}
