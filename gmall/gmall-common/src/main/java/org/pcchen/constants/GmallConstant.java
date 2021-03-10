package org.pcchen.constants;

public class GmallConstant {
    public static final String LOGGER_MOCK_DATA_POST_URL = "http://localhost:8080/log";
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";

    public static final String ES_INDEX_DAU = "gmall_dau";
    public static final String ES_INDEX_ORDER = "gmall_order";
    public static final String ES_INDEX_SALE = "gmall_sale_detail";

    /*public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties")));

        System.out.println(properties.get("redis.port"));
    }*/
}
