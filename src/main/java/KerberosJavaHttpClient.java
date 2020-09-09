import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class KerberosJavaHttpClient {
    private String principal;
    private String keyTabLocation;

    public KerberosJavaHttpClient() {}
    public KerberosJavaHttpClient(String principal, String keyTabLocation) {
        super();
        this.principal = principal;
        this.keyTabLocation = keyTabLocation;
    }

    public KerberosJavaHttpClient(String principal, String keyTabLocation, String krb5Location) {
        this(principal, keyTabLocation);
        System.setProperty("java.security.krb5.conf", krb5Location);
    }

    public KerberosJavaHttpClient(String principal, String keyTabLocation, boolean isDebug) {
        this(principal, keyTabLocation);
        if (isDebug) {
            System.setProperty("sun.security.spnego.debug", "true");
            System.setProperty("sun.security.krb5.debug", "true");
        }
    }

    public KerberosJavaHttpClient(String principal, String keyTabLocation, String krb5Location, boolean isDebug) {
        this(principal, keyTabLocation, isDebug);
        System.setProperty("java.security.krb5.conf", krb5Location);
    }

    private static HttpClient buildSpengoHttpClient() {
        HttpClientBuilder builder = HttpClientBuilder.create();
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().
                register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1, null), new Credentials() {
            public Principal getUserPrincipal() {
                return null;
            }
            public String getPassword() {
                return null;
            }
        });
        builder.setDefaultCredentialsProvider(credentialsProvider);
        CloseableHttpClient httpClient = builder.build();
        return httpClient;
    }

    public HttpResponse callRestUrl(final String url,final String userId) {
        //keyTabLocation = keyTabLocation.substring("file://".length());
        System.out.println(String.format("Calling KerberosHttpClient %s %s %s",this.principal, this.keyTabLocation, url));
        Configuration config = new Configuration() {
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] { new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<String, Object>() {
                    {
                        put("useTicketCache", "false");
                        put("useKeyTab", "true");
                        put("keyTab", keyTabLocation);
                        //Krb5 in GSS API needs to be refreshed so it does not throw the error
                        //Specified version of key is not available
                        put("refreshKrb5Config", "true");
                        put("principal", principal);
                        put("storeKey", "true");
                        put("doNotPrompt", "true");
                        put("isInitiator", "true");
                        put("debug", "true");
                    }
                }) };
            }
        };
        Set<Principal> princ = new HashSet<Principal>(1);
        princ.add(new KerberosPrincipal(userId));
        Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());
        try {
            LoginContext lc = new LoginContext("", sub, null, config);
            lc.login();
            Subject serviceSubject = lc.getSubject();
            return Subject.doAs(serviceSubject, new PrivilegedAction<HttpResponse>() {
                HttpResponse httpResponse = null;
                public HttpResponse run() {
                    try {
                        HttpUriRequest request = new HttpGet(url);
                        HttpClient spnegoHttpClient = buildSpengoHttpClient();
                        httpResponse = spnegoHttpClient.execute(request);
                        return httpResponse;
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                    return httpResponse;
                }
            });
        } catch (Exception le) {
            le.printStackTrace();;
        }
        return null;
    }

    public static void main(String[] args) throws UnsupportedOperationException, IOException {
        KerberosJavaHttpClient restTest = new KerberosJavaHttpClient("HTTP/test@test.com",
                "file://C://Development//test.keytab", true);
        HttpResponse response = restTest.callRestUrl("http://test.com/service/employees",
                "HTTP/test@test.com");
        InputStream is = response.getEntity().getContent();
        System.out.println("Status code " + response.getStatusLine().getStatusCode());
        System.out.println(Arrays.deepToString(response.getAllHeaders()));
        System.out.println(new String(IOUtils.toByteArray(is), "UTF-8"));
    }
}
