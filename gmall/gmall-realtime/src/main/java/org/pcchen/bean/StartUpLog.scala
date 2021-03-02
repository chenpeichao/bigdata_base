package org.pcchen.bean

/**
  *
  *
  * @author ceek
  * @create 2021-03-02 14:52
  **/
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var logHourMinute: String,
                      var ts: Long
                     ) {
}