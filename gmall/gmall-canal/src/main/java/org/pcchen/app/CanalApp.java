package org.pcchen.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pcchen.handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author ceek
 * @create 2021-03-08 15:25
 **/
public class CanalApp {
    public static void main(String[] args) {
        //创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("10.10.32.61", 11111), "example", "", "");

        while (true) {
            //连接、订阅 、抓取数据
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                try {
                    System.out.println("没有数据，休息一会");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断事件类型  只处理 行变化业务
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        //把数据集进行反序列化
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;

                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获得行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();   //操作类型
                        String tableName = entry.getHeader().getTableName();//表名

                        CanalHandler.handle(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }

    /**
     * 数据处理
     *
     * @param entrys
     */
    private void dataHandle(List<Entry> entrys) throws InvalidProtocolBufferException {
        for (Entry entry : entrys) {
            if (EntryType.ROWDATA == entry.getEntryType()) {
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                EventType eventType = rowChange.getEventType();
                if (eventType == EventType.DELETE) {
                    saveDeleteSql(entry);
                } else if (eventType == EventType.UPDATE) {
                    saveUpdateSql(entry);
                } else if (eventType == EventType.INSERT) {
                    saveInsertSql(entry);
                }
            }
        }
    }

    /**
     * 保存更新语句
     *
     * @param entry
     */
    private void saveUpdateSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> newColumnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("update " + entry.getHeader().getTableName() + " set ");
                for (int i = 0; i < newColumnList.size(); i++) {
                    sql.append(" " + newColumnList.get(i).getName()
                            + " = '" + newColumnList.get(i).getValue() + "'");
                    if (i != newColumnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(" where ");
                List<Column> oldColumnList = rowData.getBeforeColumnsList();
                for (Column column : oldColumnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
//                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 保存删除语句
     *
     * @param entry
     */
    private void saveDeleteSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getBeforeColumnsList();
                StringBuffer sql = new StringBuffer("delete from " + entry.getHeader().getTableName() + " where ");
                for (Column column : columnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
//                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 保存插入语句
     *
     * @param entry
     */
    private void saveInsertSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("insert into " + entry.getHeader().getTableName() + " (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append(columnList.get(i).getName());
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(") VALUES (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append("'" + columnList.get(i).getValue() + "'");
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(")");
//                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
