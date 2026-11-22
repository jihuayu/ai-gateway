package com.jihuayu.aigateway.security;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.generators.SCrypt;

public final class SecurityUtils {
    private static final Pattern KEY_RE = Pattern.compile("^[a-fA-F0-9]{64}$");
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final Base64.Encoder B64URL_ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64URL_DEC = Base64.getUrlDecoder();

    private record Cidr4(int base, int mask) {}

    private static final List<Cidr4> IPV4_BLOCKED = List.of(
        cidr4("0.0.0.0", 8),
        cidr4("10.0.0.0", 8),
        cidr4("100.64.0.0", 10),
        cidr4("127.0.0.0", 8),
        cidr4("169.254.0.0", 16),
        cidr4("172.16.0.0", 12),
        cidr4("192.0.0.0", 24),
        cidr4("192.0.2.0", 24),
        cidr4("192.168.0.0", 16),
        cidr4("198.18.0.0", 15),
        cidr4("198.51.100.0", 24),
        cidr4("203.0.113.0", 24),
        cidr4("224.0.0.0", 4),
        cidr4("240.0.0.0", 4)
    );

    private SecurityUtils() {}

    private static Cidr4 cidr4(String ip, int bits) {
        int mask = bits == 0 ? 0 : (0xffffffff << (32 - bits));
        return new Cidr4(ipv4ToInt(ip) & mask, mask);
    }

    private static int ipv4ToInt(String ip) {
        String[] parts = ip.split("\\.");
        int acc = 0;
        for (String p : parts) {
            acc = (acc << 8) | (Integer.parseInt(p) & 0xff);
        }
        return acc;
    }

    private static int ipv4BytesToInt(byte[] b) {
        return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
    }

    public static String hashKey(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static String randomBase64Url(int numBytes) {
        byte[] bytes = new byte[numBytes];
        SECURE_RANDOM.nextBytes(bytes);
        return B64URL_ENC.encodeToString(bytes);
    }

    private static byte[] keyBytes(String hexKey) {
        if (hexKey == null || !KEY_RE.matcher(hexKey).matches()) {
            throw new IllegalArgumentException("encryption key must be 64 hex characters (32 bytes)");
        }
        return HexFormat.of().parseHex(hexKey);
    }

    /** AES-256-GCM; output format: v1.<iv b64url>.<tag b64url>.<ciphertext b64url> */
    public static String encrypt(String plaintext, String hexKey) {
        byte[] key = keyBytes(hexKey);
        byte[] iv = new byte[12];
        SECURE_RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
            byte[] combined = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            int ctLen = combined.length - 16;
            byte[] ciphertext = new byte[ctLen];
            byte[] tag = new byte[16];
            System.arraycopy(combined, 0, ciphertext, 0, ctLen);
            System.arraycopy(combined, ctLen, tag, 0, 16);
            return "v1." + B64URL_ENC.encodeToString(iv) + "." + B64URL_ENC.encodeToString(tag) + "." + B64URL_ENC.encodeToString(ciphertext);
        } catch (Exception e) {
            throw new IllegalArgumentException("Encryption failed", e);
        }
    }

    public static String decrypt(String encrypted, String hexKey) {
        byte[] key = keyBytes(hexKey);
        if (encrypted == null) throw new IllegalArgumentException("invalid encrypted value");
        String[] parts = encrypted.split("\\.", -1);
        if (parts.length != 4 || !"v1".equals(parts[0])) {
            throw new IllegalArgumentException("invalid encrypted value");
        }
        try {
            byte[] iv = B64URL_DEC.decode(parts[1]);
            byte[] tag = B64URL_DEC.decode(parts[2]);
            byte[] ciphertext = B64URL_DEC.decode(parts[3]);
            if (iv.length != 12 || tag.length != 16) {
                throw new IllegalArgumentException("invalid encrypted value");
            }
            byte[] combined = new byte[ciphertext.length + 16];
            System.arraycopy(ciphertext, 0, combined, 0, ciphertext.length);
            System.arraycopy(tag, 0, combined, ciphertext.length, 16);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
            byte[] plain = cipher.doFinal(combined);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid encrypted value", e);
        }
    }

    public static String hashPassword(String password) {
        byte[] salt = new byte[16];
        SECURE_RANDOM.nextBytes(salt);
        byte[] derived = SCrypt.generate(password.getBytes(StandardCharsets.UTF_8), salt, 16384, 8, 1, 64);
        return "scrypt:" + HexFormat.of().formatHex(salt) + ":" + HexFormat.of().formatHex(derived);
    }

    public static boolean verifyPassword(String password, String stored) {
        if (stored == null) return false;
        String[] parts = stored.split(":");
        if (parts.length != 3 || !"scrypt".equals(parts[0])) return false;
        try {
            byte[] salt = HexFormat.of().parseHex(parts[1]);
            byte[] expected = HexFormat.of().parseHex(parts[2]);
            byte[] derived = SCrypt.generate(password.getBytes(StandardCharsets.UTF_8), salt, 16384, 8, 1, expected.length);
            return MessageDigest.isEqual(expected, derived);
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    public static boolean isBlockedAddress(String addressLiteral) {
        if (addressLiteral == null || addressLiteral.isBlank()) return true;
        String clean = addressLiteral.trim().replaceAll("^\\[|\\]$", "");
        String lower = clean.toLowerCase();
        if (lower.startsWith("::ffff:")) {
            String suffix = lower.substring(7);
            if (suffix.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$")) {
                return isIpv4Blocked(ipv4ToInt(suffix));
            }
        }
        try {
            InetAddress addr = InetAddress.getByName(clean);
            return isBlockedInetAddress(addr);
        } catch (Exception e) {
            return true;
        }
    }

    public static boolean isBlockedInetAddress(InetAddress addr) {
        byte[] raw = addr.getAddress();
        if (addr instanceof Inet4Address || raw.length == 4) {
            return isIpv4Blocked(ipv4BytesToInt(raw));
        }
        if (addr instanceof Inet6Address && raw.length == 16) {
            // Check IPv4-mapped IPv6 ::ffff:0:0/96
            boolean mapped = true;
            for (int i = 0; i < 10; i++) {
                if (raw[i] != 0) { mapped = false; break; }
            }
            if (mapped && (raw[10] & 0xff) == 0xff && (raw[11] & 0xff) == 0xff) {
                int v4 = ((raw[12] & 0xff) << 24) | ((raw[13] & 0xff) << 16) | ((raw[14] & 0xff) << 8) | (raw[15] & 0xff);
                return isIpv4Blocked(v4);
            }
            if (addr.isAnyLocalAddress() || addr.isLoopbackAddress() || addr.isLinkLocalAddress() || addr.isMulticastAddress()) {
                return true;
            }
            int first = ((raw[0] & 0xff) << 8) | (raw[1] & 0xff);
            int second = ((raw[2] & 0xff) << 8) | (raw[3] & 0xff);
            if ((first & 0xfe00) == 0xfc00) return true; // fc00::/7 ULA
            if ((first & 0xffc0) == 0xfe80) return true; // link-local
            if ((first & 0xff00) == 0xff00) return true; // multicast
            if (first == 0x2001 && second == 0x0db8) return true; // documentation
            return (first & 0xe000) != 0x2000; // only global unicast 2000::/3
        }
        return true;
    }

    private static boolean isIpv4Blocked(int ipInt) {
        for (Cidr4 cidr : IPV4_BLOCKED) {
            if ((ipInt & cidr.mask()) == cidr.base()) {
                return true;
            }
        }
        return false;
    }

    public static List<InetAddress> validateProviderUrl(String url, boolean allowPrivate) {
        if (url == null || url.isBlank()) {
            throw new ProviderUrlException("provider URL is invalid");
        }
        URI parsed;
        try {
            parsed = new URI(url.trim());
        } catch (Exception e) {
            throw new ProviderUrlException("provider URL is invalid");
        }
        String scheme = parsed.getScheme() != null ? parsed.getScheme().toLowerCase() : "";
        if (!"https".equals(scheme) && !(allowPrivate && "http".equals(scheme))) {
            throw new ProviderUrlException("provider URL must use HTTPS");
        }
        if (parsed.getRawUserInfo() != null) {
            throw new ProviderUrlException("provider URL must not contain credentials");
        }
        if (parsed.getRawFragment() != null) {
            throw new ProviderUrlException("provider URL must not contain a fragment");
        }
        String rawAuthority = parsed.getRawAuthority();
        if (rawAuthority != null && rawAuthority.contains("@")) {
            throw new ProviderUrlException("provider URL must not contain credentials");
        }
        String host = parsed.getHost();
        if (host == null || host.isBlank()) {
            throw new ProviderUrlException("provider URL is invalid");
        }
        String normHost = host.toLowerCase().replaceAll("^\\[|\\]$", "").replaceAll("\\.$", "");
        if (normHost.isEmpty() || "localhost".equals(normHost) || normHost.endsWith(".localhost")) {
            if (!allowPrivate) {
                throw new ProviderUrlException("provider URL host is not allowed");
            }
        }
        InetAddress[] resolved;
        try {
            resolved = InetAddress.getAllByName(normHost);
        } catch (UnknownHostException e) {
            throw new ProviderUrlException("provider host could not be resolved");
        }
        if (resolved == null || resolved.length == 0) {
            throw new ProviderUrlException("provider host could not be resolved");
        }
        List<InetAddress> usable = new ArrayList<>();
        for (InetAddress addr : resolved) {
            if (!allowPrivate && isBlockedInetAddress(addr)) {
                throw new ProviderUrlException("provider URL resolves to a private or reserved address");
            }
            usable.add(addr);
        }
        return usable;
    }

    public static class ProviderUrlException extends RuntimeException {
        public final int statusCode = 400;
        public final String code = "invalid_provider_url";

        public ProviderUrlException(String message) {
            super(message);
        }
    }
}
