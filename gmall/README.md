索引创建语句：
#日活
PUT gmall_dau
{
	"mappings": {
		"_doc": {
			"properties": {
				"mid": {
					"type": "keyword"
				},
				"uid": {
					"type": "keyword"
				},
				"area": {
					"type": "keyword"
				},
				"os": {
					"type": "keyword"
				},
				"ch": {
					"type": "keyword"
				},
				"vs": {
					"type": "keyword"
				},
				"logDate": {
					"type": "keyword"
				},
				"logHour": {
					"type": "keyword"
				},
				"logHourMinute": {
					"type": "keyword"
				},
				"ts": {
					"type": "long"
				}
			}
		}
	}
}


#订单实时canal监控
PUT gmall_order
{
	"mappings": {
		"_doc": {
			"properties": {
				"provinceId": {
					"type": "keyword"
				},
				"consignee": {
					"type": "keyword",
					"index": false
				},
				"consigneeTel": {
					"type": "keyword",
					"index": false
				},
				"createDate": {
					"type": "keyword"
				},
				"createHour": {
					"type": "keyword"
				},
				"createHourMinute": {
					"type": "keyword"
				},
				"createTime": {
					"type": "keyword"
				},
				"deliveryAddress": {
					"type": "keyword"
				},
				"expireTime": {
					"type": "keyword"
				},
				"id": {
					"type": "keyword"
				},
				"imgUrl": {
					"type": "keyword",
					"index": false
				},
				"operateTime": {
					"type": "keyword"
				},
				"orderComment": {
					"type": "keyword",
					"index": false
				},
				"orderStatus": {
					"type": "keyword"
				},
				"outTradeNo": {
					"type": "keyword",
					"index": false
				},
				"parentOrderId": {
					"type": "keyword"
				},
				"paymentWay": {
					"type": "keyword"
				},
				"totalAmount": {
					"type": "double"
				},
				"trackingNo": {
					"type": "keyword"
				},
				"tradeBody": {
					"type": "keyword",
					"index": false
				},
				"userId": {
					"type": "keyword"
				}
			}
		}
	}
}