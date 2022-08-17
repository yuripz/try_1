package net.plumbing.msgbus.ws.client.ssl;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import net.plumbing.msgbus.ws.client.core.Security;

import javax.net.ssl.*;
import java.security.*;
import java.util.ArrayList;
import java.util.List;

/**
 */
@SuppressWarnings({"unused", "WeakerAccess","deprecation"})
public class SSLUtils {

    public static X509TrustManager getTrustManager(KeyStore trustStore) throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return (X509TrustManager) trustManagerFactory.getTrustManagers()[0];
    }

    public static X509KeyManager getKeyManager(KeyStore keyStore, char[] keyStorePassword) throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword);
        return (X509KeyManager) keyManagerFactory.getKeyManagers()[0];
    }

    public static X509TrustManager getMultiTrustManager(X509TrustManager... managers) {
        List<X509TrustManager> managersList = new ArrayList<X509TrustManager>();
        for (X509TrustManager manager : managers) {
            managersList.add(manager);
        }
        return new MultiX509TrustManager(managersList);
    }

    public static SSLSocketFactory getMergedSocketFactory(Security securityOne, Security securityTwo) throws GeneralSecurityException {
        X509KeyManager keyManagerOne = getKeyManager(securityOne.getKeyStore(), securityOne.getKeyStorePassword());
        X509KeyManager keyManagerTwo = getKeyManager(securityTwo.getKeyStore(), securityTwo.getKeyStorePassword());

        X509TrustManager trustManager = getMultiTrustManager(
                getTrustManager(securityOne.getTrustStore()),
                getTrustManager(securityTwo.getTrustStore())
        );

        SSLContext context = SSLContext.getInstance(securityOne.getSslContextProtocol());
        boolean strictHostVerification = securityOne.isStrictHostVerification() && securityTwo.isStrictHostVerification();

        context.init(new KeyManager[]{keyManagerOne, keyManagerTwo}, new TrustManager[]{trustManager}, new SecureRandom());
        X509HostnameVerifier verifier = strictHostVerification ?
                SSLSocketFactory.STRICT_HOSTNAME_VERIFIER : SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;
        return new SSLSocketFactory(context, verifier);
    }

    public static SSLSocketFactory getFactory(Security security) throws GeneralSecurityException {
        X509HostnameVerifier verifier = security.isStrictHostVerification() ?
                SSLSocketFactory.STRICT_HOSTNAME_VERIFIER : SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;
        SSLSocketFactory socketFactory = new SSLSocketFactory(security.getSslContextProtocol(),
                security.getKeyStore(), security.getKeyStorePasswordAsString(),
                security.getTrustStore(), new SecureRandom(), null, verifier);
        return socketFactory;
    }


}
