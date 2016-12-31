import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
var cal = Calendar.getInstance()
cal.add(Calendar.DATE,-1)
var yesterday = dateformat.format(cal.getTime())

var hc = new org.apache.spark.sql.hive.HiveContext(sc)

var xjData = hc.sql("select bicc.calling_number,bicc.called_number,2 as type,bicc.calling_number as xj_phone,bicc.called_number as province_phone from ren.bicc_isup as bicc,ren.xjindex as xjindex where substr(bicc.calling_number,-11,7)=xjindex.section union all select bicc.calling_number,bicc.called_number,1 as type,bicc.called_number as xj_phone,bicc.calling_number as province_phone from ren.bicc_isup as bicc,ren.xjindex as xjindex where substr(bicc.called_number,-11,7)=xjindex.section")
xjData.registerTempTable("xjData")

var addProvince = hc.sql("select xjData.*,phoneindex.province from xjData,ren.phoneindex where substr(xjData.province_phone,-11,7)=phoneindex.section")
addProvince.registerTempTable("addProvince")


var formatData = hc.sql("select province as code,calling_number as phone,type,count(*) as number,'"+ yesterday +"' as create_time from addProvince group by province,calling_number,type")
formatData.registerTempTable("formatData")

var statistics = hc.sql("select code,phone,type,number,create_time from (select code,phone,type,number,create_time,row_number() OVER (PARTITION BY code,phone,type ORDER BY number DESC) rank from formatData) tmp where rank<=50").show
