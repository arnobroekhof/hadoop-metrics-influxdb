package org.apache.hadoop.metrics2.sink.influxdb.utils;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Class for connecting and writing metrics to influxdb
 */
public class InfluxDBService {

    private static final Log LOG = LogFactory.getLog(InfluxDBService.class);
    private String url;
    private String username;
    private String password;
    private String database;
    private HttpClient httpClient;

    /**
     * Default constructor
     *
     * @param url      of influxdb
     * @param database name to write to
     * @param username to use for writing
     * @param password to use for writing
     */
    public InfluxDBService(final String url, final String database, final String username, final String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;

        httpClient = new DefaultHttpClient();
    }

    /**
     * Write metrics to influx.
     * Influx assumes a String with the following format
     * metric,tag-key=tag-value value=value timestamp
     *
     * @param lines String
     * @return return boolean if the write http return code from influx equals 204
     * @throws IOException
     */
    public boolean write(final String lines) throws IOException {
        boolean _return;
        //String influxUrl = url + "/write?db=" + URLEncoder.encode(database, "UTF-8");
        String influxUrl = url + "/write" + "" +
            "?db=" + URLEncoder.encode(database, "UTF-8") +
            "&u=" + URLEncoder.encode(username, "UTF-8") +
            "&p=" + URLEncoder.encode(password, "UTF-8");
        HttpPost request = new HttpPost(influxUrl);
        request.setEntity(new ByteArrayEntity(
            lines.getBytes("UTF-8")
        ));
        HttpResponse response = httpClient.execute(request);

        // consume entity and do nothing with it because we only
        // want the connection to be dropped.
        int statusCode = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());

        if (statusCode != 204) {
            LOG.error("Unable to write or parse: \n" + lines + "\n");
            throw new IOException("Error writing metrics influxdb statuscode = " + statusCode);
        }
        else {
            _return = true;
        }

        return _return;
    }
}
