
import java.text.SimpleDateFormat
import java.util.Calendar

var dateformat = new SimpleDateFormat("yyyyMMdd")
var hourformat = new SimpleDateFormat("HH")
var cal = Calendar.getInstance()
cal.add(Calendar.HOUR_OF_DAY,-2)
var today = dateformat.format(cal.getTime())
var lastTwoHour = hourformat.format(cal.getTime())
var location = "/tmp/nx_url_result_hdfs/"
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
var alldf = hc.sql("select split(a.host,'\\\\.') as arr,size(split(a.host,'\\\\.')) as sarr,split(a.host,':')[0] as host,concat(a.dstip,'\t',a.host,'\t','http://',a.host,a.url) as url from mdss.url_hour_partition as a where a.date='"+ today +"' and a.hour='"+ lastTwoHour +"' group by a.dstip,a.host,a.url")
alldf.registerTempTable("urlHourData")

var domaindf = hc.sql("select domain from mdss.domains")
domaindf.registerTempTable("domainData")

var eql2df = hc.sql("select a.url from urlHourData as a ,domainData as b where substr(a.host,-(length(a.arr[size(a.arr)-1])+length(a.arr[size(a.arr)-2])+2)) = b.domain")

var eql3df = hc.sql("select a.url from urlHourData as a ,domainData as b where substr(a.host,-(length(a.arr[size(a.arr)-1])+length(a.arr[size(a.arr)-2])+length(a.arr[size(a.arr)-3])+3)) = b.domain")
var result = alldf.select("url").except(eql2df.select("url")).except(eql3df.select("url"))

result.repartition(1).rdd.saveAsTextFile(location.concat(today).concat("-").concat(lastTwoHour))


sc.stop
exit
