/**
 * <b>ATMOSPHERE</b> - http://www.atmosphere-eubrazil.eu/
 * ***
 * <p>
 * <b>Trustworthiness Monitoring & Assessment Framework</b>
 * Component: Monitor - Client
 * <p>
 * Repository: https://github.com/eubr-atmosphere/tma-framework
 * License: https://github.com/eubr-atmosphere/tma-framework/blob/master/LICENSE
 * <p>
 * <p>
 */
package eu.atmosphere.tmaf.monitor.client;

import eu.atmosphere.tmaf.monitor.message.Message;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.security.cert.X509Certificate;
import java.security.*;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 *
 * <p>
 * <p>
 *
 * @author Nuno Antunes <nmsa@dei.uc.pt>
 */
public abstract class MonitorClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorClient.class);

    private static final String DEFAULT_ENDPOINT = "https://192.168.122.240:32025/monitor";
//    private static final String DEFAULT_ENDPOINT = "http://localhost:80/monitor";
    private static final String ENDPOINT_AUTH_PATH = "auth";
    private static final int NONINITIALIZED_PROBEID = -1;
    private static AtomicInteger serial = new AtomicInteger();

    /**
     *
     */
    private final String endpoint;
    private final Client client;
    private final AtomicInteger probeId;
    private final WebTarget target;
    private final Lock mutex;

    protected MonitorClient() {
        this(DEFAULT_ENDPOINT);
    }

    public MonitorClient(String endpoint) {
        LOGGER.info("MonitorClient {}!", endpoint);
        this.endpoint = endpoint;
        this.client = ClientBuilder.newClient();
        this.target = client.target(this.endpoint);
        this.probeId = new AtomicInteger(NONINITIALIZED_PROBEID);
        this.mutex = new ReentrantLock();
        LOGGER.trace("WebTarget created: {}", target);

    }

    /**
     *
     *
     *
     * @return if the
     */
    public final boolean isInitialized() {
        return probeId.get() != NONINITIALIZED_PROBEID;
    }


    /**
     *
     * @return a new {@link Message}
     */
    public final Message createMessage() {
        Message m = new Message();
        if (isInitialized()) {
            m.setProbeId(probeId.get());
        }
        return m;
    }

    public final boolean authenticate(int probeId, byte[] password) {
        WebTarget authTarget = target.path(ENDPOINT_AUTH_PATH);
        LOGGER.trace("authenticate target: {}", authTarget);
        LOGGER.warn("Authentication is yet to be supported.");
        return this.probeId.compareAndSet(NONINITIALIZED_PROBEID, probeId);
    }
    static {
      disableSslVerification();
    }

    private static void disableSslVerification() {
      try
      {
          // Create a trust manager that does not validate certificate chains
          TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        }
        };

          // Install the all-trusting trust manager
          SSLContext sc = SSLContext.getInstance("SSL");
          sc.init(null, trustAllCerts, new java.security.SecureRandom());
          HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

          // Create all-trusting host name verifier
          HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

          // Install the all-trusting host verifier
          HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
      } catch (NoSuchAlgorithmException e) {
          e.printStackTrace();
      } catch (KeyManagementException e) {
          e.printStackTrace();
      }
    }


    protected final int post(Message message) {
        final long sentTime = System.currentTimeMillis();
        message.setSentTime(sentTime);

        LOGGER.trace("Post: {}", message);
        int result = -1;
        final Invocation.Builder request = target.request(MediaType.APPLICATION_JSON_TYPE);
        final Entity<Message> entity = Entity.json(message);

        mutex.lock();
        try {
            String response = request.post(entity, String.class);
            LOGGER.debug("Response: {}", response);
            if (response != null) {
                result = response.length();
            } else {
                LOGGER.warn("Unexpected response: {}", response);
            }
        } finally {
            mutex.unlock();
        }
        LOGGER.trace("result: {}", result);
        return result;
    }

    /**
     * FIXME: Complete Javadoc
     *
     *
     * @param message
     * @return if the
     */
    protected abstract int dispatch(Message message);

    public final int send(Message message) {
        final int messageId = probeId.get() + serial.incrementAndGet();
        message.setMessageId(messageId);
        return dispatch(message);
    }
}
