import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyyMMdd")
var cal = Calendar.getInstance()
cal.add(Calendar.DATE,-1)
var yesterday = dateformat.format(cal.getTime())
var location = "/tmp/url/result/"

val hc = new org.apache.spark.sql.hive.HiveContext(sc)
val df = hc.sql("select first(a.host) as host,first(a.dstip) as ip,first(a.ip) as source_ip,first(a.getorpost) as method,concat('http://',first(a.host),first(a.url)) as url,'' as post_data,'' as headers,'' as user_agent,'' as cookie from mdss.url_hour_partition as a, idriller_panalog.iptable2 as b where a.getorpost='GET' and a.date='"+ yesterday +"' and a.host=b.host group by a.host ,a.url")

df.toJSON.repartition(1).saveAsTextFile(location.concat(yesterday))
sc.stop
exit
