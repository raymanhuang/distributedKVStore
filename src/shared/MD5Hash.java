package shared;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.*;

public class MD5Hash {
    public static String hashString(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(input.getBytes());
        byte[] digest = md.digest();
        
        // Convert byte array into signum representation
        BigInteger no = new BigInteger(1, digest);
        
        // Convert message digest into hex value
        String hashtext = no.toString(16);
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        return hashtext;
    }
}

