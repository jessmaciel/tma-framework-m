/**
 * <b>ATMOSPHERE</b> - http://www.atmosphere-eubrazil.eu/
 * ***
 * <p>
 * <b>Trustworthiness Monitoring & Assessment Framework</b>
 * Component: // FIXME: Define associated component
 * <p>
 * Repository: https://github.com/eubr-atmosphere/tma-framework
 * License: https://github.com/eubr-atmosphere/tma-framework/blob/master/LICENSE
 * <p>
 * <p>
 */
package eubr.atmosphere.tma.probes;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;

import eu.atmosphere.tmaf.monitor.client.BackgroundClient;
import eu.atmosphere.tmaf.monitor.message.Data;
import eu.atmosphere.tmaf.monitor.message.Message;
import eu.atmosphere.tmaf.monitor.message.Observation;
import eubr.atmosphere.tma.probes.utils.PropertiesManager;

/**
 * Probe that collects the data from kubernetes pods
 * <p>
 *
 * @author Jose DAbruzzo Pereira <josep@dei.uc.pt>
 */
public class ProbeKubernetes {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProbeKubernetes.class);

    private static final int cpuDescriptionId = 27;
    private static final int memoryDescriptionId = 28;

    private static final int probeId = 7;

    private static final String endpoint = PropertiesManager.getInstance().getProperty("MONITOR_ENDPOINT");
    private static final String metricsEndpoint =
            PropertiesManager.getInstance().getProperty("METRICS_ENDPOINT");

    private static final String namespaceName = "default";

    private static Map<String, Integer> resourceKeyMap = new HashMap<String, Integer>();

    private static int messageId = 0;

    public static void main(String[] args) {

        LOGGER.info("Trust me! This is ATMOSPHERE!");
        BackgroundClient client = new BackgroundClient(endpoint);

        client.authenticate(probeId, "pass".getBytes());

        boolean start = client.start();
        LOGGER.info("start {}!", start);

        //match between name pod | node and ID resource
        resourceKeyMap.put("kafka-0", 8);
        resourceKeyMap.put("teastore-webui-0", 9);
        resourceKeyMap.put("teastore-webui-1", 10);
        resourceKeyMap.put("teastore-webui-2", 11);
        // resourceKeyMap.put("teastore-webui-3", 12);
        // resourceKeyMap.put("teastore-webui-4", 13);
        // resourceKeyMap.put("teastore-webui-5", 14);
        // resourceKeyMap.put("teastore-webui-6", 15);
        // resourceKeyMap.put("teastore-persistence-0", 16);
        // resourceKeyMap.put("teastore-persistence-1", 17);
        // resourceKeyMap.put("teastore-persistence-2", 18);
        // resourceKeyMap.put("teastore-persistence-3", 19);
        // resourceKeyMap.put("teastore-image-0", 20);
        // resourceKeyMap.put("teastore-image-1", 21);
        // resourceKeyMap.put("teastore-auth-0", 22);
        // resourceKeyMap.put("teastore-auth-1", 23);
        resourceKeyMap.put("master", 24);
        resourceKeyMap.put("worker", 25);



        // resourceKeyMap.put("virtmanagernode-standard-pc-i440fx-piix-1996", 11);
        // resourceKeyMap.put("virtmanagermaster-standard-pc-i440x-piix-1996", 12);

        String uriPods = metricsEndpoint + "namespaces/" + namespaceName + "/pods/";
        String uriNodes = metricsEndpoint + "nodes/";
        HttpResponse response;
        InputStreamReader isr;
        try {
            while (true) {
                response = requestRestService(uriPods);
                isr = new InputStreamReader(response.getEntity().getContent());
                parsePodMetrics(isr, client);

                response = requestRestService(uriNodes);
                isr = new InputStreamReader(response.getEntity().getContent());
                parseNodeMetrics(isr, client);

                // This is the frequency that the probe collect the data and send to the monitor
                Thread.sleep(5000);
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(20000);
            boolean stop = client.stop();
            LOGGER.info("stop {}!", stop);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        } catch (ExecutionException ex) {
            ex.printStackTrace();
        }

        client.shutdown();
        LOGGER.info("Trust me! This is ATMOSPHERE!");
    }

    private static HttpResponse requestRestService(String uri) throws ClientProtocolException, IOException {
      HttpClient client = new DefaultHttpClient();
      HttpGet request = new HttpGet(uri);
      HttpResponse response = client.execute(request);

      return response;
    }

    private static int getResourceId(String podName) {
        if (resourceKeyMap.containsKey(podName)) {
            return resourceKeyMap.get(podName);
        } else {
            return -1;
        }
    }

    private static void parsePodMetrics(InputStreamReader isr, BackgroundClient client) {

        Gson gson = new GsonBuilder().create();
        Object rawJson = gson.fromJson(isr, Object.class);
        LinkedTreeMap<String, Object> c = (LinkedTreeMap<String, Object>) rawJson;
        List<Object> items = (List<Object>) c.get("items");

        // TODO change to check the node that the pod is inserted. We are allowed to do that, since we have only one node
        // http://192.168.122.34:8089/api/v1/namespaces/default/pods/[POD_NAME]

        for (Object object : items) {
            LinkedTreeMap<String, Object> podData = (LinkedTreeMap<String, Object>) object;
            Object metadata = podData.get("metadata");
            LinkedTreeMap<String, Object> ltmMetadata = (LinkedTreeMap<String, Object>) metadata;
            String podName = ltmMetadata.get("name").toString();

            if (isMonitorizedPod(podName)) {
                Message message = client.createMessage();
                int resourceId = getResourceId(podName);
                if (resourceId == -1) {
                    LOGGER.info("ResourceId not found: {}", podName);
                    continue;
                }
                message.setResourceId(resourceId);

                List<Object> containers = (List<Object>) podData.get("containers");
                for (Object container: containers) {

                    LinkedTreeMap<String, Object> ltmContainers = (LinkedTreeMap<String, Object>) container;
                    LinkedTreeMap<String, Object> ltmUsage = (LinkedTreeMap<String, Object>) ltmContainers.get("usage");

                    String cpuString = ltmUsage.get("cpu").toString();
                    message.addData(parseDatumValue(cpuString, cpuDescriptionId, 1));

                    String memoryString = ltmUsage.get("memory").toString();
                    message.addData(parseDatumValue(memoryString, memoryDescriptionId, 2));
                }
                message.setSentTime(Calendar.getInstance().getTimeInMillis());
                message.setMessageId(messageId++);
                LOGGER.info(message.toString());
                client.send(message);
            }
        }
    }

    private static void parseNodeMetrics(InputStreamReader isr, BackgroundClient client) {

        Gson gson = new GsonBuilder().create();
        Object rawJson = gson.fromJson(isr, Object.class);
        LinkedTreeMap<String, Object> c = (LinkedTreeMap<String, Object>) rawJson;
        List<Object> items = (List<Object>) c.get("items");

        for (Object object : items) {
            LinkedTreeMap<String, Object> nodeData = (LinkedTreeMap<String, Object>) object;
            Object metadata = nodeData.get("metadata");
            LinkedTreeMap<String, Object> ltmMetadata = (LinkedTreeMap<String, Object>) metadata;
            String nodeName = ltmMetadata.get("name").toString();

            if (isMonitorizedNode(nodeName)) {
                LinkedTreeMap<String, Object> ltmUsage = (LinkedTreeMap<String, Object>) nodeData.get("usage");

                Message message = client.createMessage();
                int resourceId = getResourceId(nodeName);
                message.setResourceId(resourceId);

                String cpuString = ltmUsage.get("cpu").toString();
                message.addData(parseDatumValue(cpuString, cpuDescriptionId, 1));

                String memoryString = ltmUsage.get("memory").toString();
                message.addData(parseDatumValue(memoryString, memoryDescriptionId, 2));

                message.setSentTime(Instant.now().getEpochSecond());
                message.setMessageId(messageId++);

                LOGGER.info(message.toString());
                client.send(message);
            }
        }
    }

    private static Data parseDatumValue(String valueString, int descriptionId, int unitLength) {
        int value = 0;
        if (!"0".equals(valueString.trim()) && !valueString.isEmpty() && (valueString.length() > unitLength)) {
            value = Integer.parseInt(valueString.substring(0, valueString.length() - unitLength));
        }
        Data datum = new Data(Data.Type.MEASUREMENT, descriptionId,
                new Observation(Instant.now().getEpochSecond(), value));
        return datum;
    }

    private static boolean isMonitorizedPod(String podName) {
        return podName.startsWith("mysql-wsvd-") || podName.startsWith("kafka-") || podName.startsWith("teastore-webui-") || podName.startsWith("teastore-persistence-") || podName.startsWith("teastore-image-") || podName.startsWith("teastore-auth-") ;
    }

    private static boolean isMonitorizedNode(String nodeName) {
        return nodeName.startsWith("master") || nodeName.startsWith("worker");
    }
}