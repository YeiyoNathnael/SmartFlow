package nathnael.abatye.adu.ac.ae.handlers;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class SecurityUtils {

    private static final String SHARED_KEY_PATH = "src/main/resources/shared_aes.key";
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private SecurityUtils() {
    }

    public static String encryptPayload(String payload) throws Exception {
        byte[] keyBytes = loadSharedKey();
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");

        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        new SecureRandom().nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);

        byte[] cipherText = cipher.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        byte[] combined = new byte[iv.length + cipherText.length];

        System.arraycopy(iv, 0, combined, 0, iv.length);
        System.arraycopy(cipherText, 0, combined, iv.length, cipherText.length);

        return Base64.getEncoder().encodeToString(combined);
    }

    public static String decryptPayload(String encryptedPayload) throws Exception {
        byte[] keyBytes = loadSharedKey();
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");

        byte[] combined = Base64.getDecoder().decode(encryptedPayload);
        if (combined.length <= GCM_IV_LENGTH_BYTES) {
            throw new IllegalArgumentException("Encrypted payload is too short");
        }

        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        byte[] cipherText = new byte[combined.length - GCM_IV_LENGTH_BYTES];

        System.arraycopy(combined, 0, iv, 0, iv.length);
        System.arraycopy(combined, iv.length, cipherText, 0, cipherText.length);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);

        byte[] plainText = cipher.doFinal(cipherText);
        return new String(plainText, StandardCharsets.UTF_8);
    }

    public static boolean verifyIntegrity(String encryptedPayload) {
        try {
            byte[] keyBytes = loadSharedKey();
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");

            byte[] combined = Base64.getDecoder().decode(encryptedPayload);
            if (combined.length <= GCM_IV_LENGTH_BYTES) {
                return false;
            }

            byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
            byte[] cipherText = new byte[combined.length - GCM_IV_LENGTH_BYTES];

            System.arraycopy(combined, 0, iv, 0, iv.length);
            System.arraycopy(combined, iv.length, cipherText, 0, cipherText.length);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);

            // doFinal validates the GCM tag; plaintext is intentionally ignored.
            cipher.doFinal(cipherText);
            return true;
        } catch (Exception integrityError) {
            return false;
        }
    }

    private static byte[] loadSharedKey() throws Exception {
        String base64Key = new String(Files.readAllBytes(Paths.get(SHARED_KEY_PATH)), StandardCharsets.UTF_8).trim();
        byte[] keyBytes = Base64.getDecoder().decode(base64Key);

        if (keyBytes.length != 16 && keyBytes.length != 24 && keyBytes.length != 32) {
            throw new IllegalStateException("Invalid AES key length in " + SHARED_KEY_PATH);
        }
        return keyBytes;
    }
}
