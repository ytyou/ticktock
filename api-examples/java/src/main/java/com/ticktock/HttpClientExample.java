package com.ticktock;

import okhttp3.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import com.alibaba.fastjson.JSON;

public class HttpClientExample {
    protected String writeURL = "/api/put";
    protected String queryURL = "/api/query";
    protected String METRIC = "test.metric";
    protected String FARM_TAG = "farm";
    protected String DEVICE_TAG = "device";
    protected String SENSOR_TAG = "sensor";
    protected MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
    protected static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private long timestamp = 1614735960L;

    protected static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
        .readTimeout(500000, TimeUnit.MILLISECONDS)
        .connectTimeout(500000, TimeUnit.MILLISECONDS)
        .writeTimeout(500000, TimeUnit.MILLISECONDS)
        .build();

    private static OkHttpClient getOkHttpClient() {
        return OK_HTTP_CLIENT;
    }

    private int exeOkHttpRequest(Request request) {
        Response response = null;
        OkHttpClient client = getOkHttpClient();
        try {
            response = client.newCall(request).execute();
            int code = response.code();
            if (!response.isSuccessful()) {
                System.out.println("Fail with code " + code);
            } else {
                System.out.println("Succeed with code " + code);
                System.out.println(response.body().string());
            }
            return code;
        } catch (Exception e) {
            e.printStackTrace();
            return 500;
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private void initConnect(String ip, String port) {
        writeURL = "http://" + ip + ":" + port + "" + writeURL;
        queryURL = "http://" + ip + ":" + port + "" + queryURL;
    }

    /**
     * insert data point in plain format (default and recommended)
     */
    private int insertPlainData() {
        StringBuilder putReqSB = new StringBuilder();

        int numDataPoints = 2;
        for(int i=1; i <= numDataPoints; i++) {
            putReqSB.append("put " + METRIC);
            putReqSB.append(" " + timestamp);
            putReqSB.append(" " + 1.0);
            putReqSB.append(" " + FARM_TAG + "=f" + i);
            putReqSB.append(" " + DEVICE_TAG + "=d" + i);
            putReqSB.append(" " + SENSOR_TAG + "=s" + i);
            putReqSB.append(System.lineSeparator());
        }

        System.out.println("To insert:");
        System.out.println(putReqSB.toString());
        return execQuery(writeURL, putReqSB.toString());
    }

    /**
     * Insert data point in Json format
     */
    private int insertJsonData() {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        int numDataPoints = 2;
        for(int i=1; i <= numDataPoints; i++) {
            Map<String, Object> pointMap = new HashMap<>();
            pointMap.put("metric", METRIC);
            pointMap.put("timestamp", timestamp);
            pointMap.put("value", i);
            Map<String, Object> Tags = new HashMap<>();
            Tags.put(FARM_TAG, "f"+i);
            Tags.put(DEVICE_TAG, "d"+i);
            Tags.put(SENSOR_TAG, "s"+i);
            pointMap.put("tags", Tags);
            list.add(pointMap);
        }
        String json = JSON.toJSONString(list);
        return execQuery(writeURL, json);
    }


    private int execQuery(String reqURL, String query) {
        Request request = new Request.Builder()
            .url(reqURL)
            .post(RequestBody.create(MEDIA_TYPE_TEXT, query))
            .build();
        return exeOkHttpRequest(request);
    }

    public long query(long start, long end) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("start", start);
        map.put("end", end);
        Map<String, Object> subQuery = new HashMap<String, Object>();
        subQuery.put("aggregator", "avg");
        subQuery.put("downsample", "1m-avg");
        subQuery.put("metric", METRIC);
        Map<String, Object> subTag = new HashMap<String, Object>();
        subTag.put(FARM_TAG, "f1");
        subTag.put(DEVICE_TAG, "d1");
        subTag.put(SENSOR_TAG, "s1");
        subQuery.put("tags", subTag);
        List<Map<String, Object>> list2 = new ArrayList<>();
        list2.add(subQuery);
        map.put("queries", list2);

        String json = JSON.toJSONString(map);
        System.out.println("To query:");
        System.out.println(json);

        return execQuery(queryURL, json);
    }

    public static void main(String[] args) {
        // Usgae: java HttpClientExample <host> <port>
	HttpClientExample a=new HttpClientExample();
        String host=args[0];
        String port=args[1];
	a.initConnect(host, port);

	// By default, Ticktock accepts data points in plain format(http.request.format=plain in config).
	a.insertPlainData();

	// You need to change TickTock config, http.request.format=json
        //a.insertJsonData();

        a.query(a.timestamp, 1614739000L);
    }
}

