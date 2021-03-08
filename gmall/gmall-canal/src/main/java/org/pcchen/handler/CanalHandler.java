package org.pcchen.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.pcchen.constants.GmallConstant;
import org.pcchen.util.MyKafkaSender;

import java.util.List;

/**
 * @author ceek
 * @create 2021-03-08 15:26
 **/
public class CanalHandler {
    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //下单操作
            for (CanalEntry.RowData rowData : rowDatasList) {  //行集展开
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {  //列集展开
                    System.out.println(column.getName() + ":::" + column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertyName, column.getValue());
                }
//                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }

        }


    }
}
