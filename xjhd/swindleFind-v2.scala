import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyyMMdd")
var cal = Calendar.getInstance()
var today = dateformat.format(cal.getTime())

var hc = new org.apache.spark.sql.hive.HiveContext(sc)
//  过滤新疆的数据，为了避免使用or，这里用的是union all。where条件是判断呼出呼入号码有一个为新疆的数据。注意这里的号码段表用的是phoneindex，应该修改为xinjiang_phoneindex
var xjData = hc.sql("select from_unixtime(ceil(i.btime/1000),'yyyy-MM-dd-HH') as time_Stamp,i.calling_number,i.called_number from ren.bicc_isup_less as i,ren.phoneindex as h where substr(i.calling_number,-11,7)=h.section union all select from_unixtime(ceil(i.btime/1000),'yyyy-MM-dd-HH') as time_Stamp,i.calling_number,i.called_number from ren.bicc_isup_less as i,ren.phoneindex as h where substr(i.called_number,-11,7)=h.section ")
xjData.registerTempTable("xjData")

// 统计每个号码呼出的次数
var callout = hc.sql("select count(*) as callOutCount,calling_number as phoneNum from xjData group by calling_number")
callout.registerTempTable("callout")

// 统计每个号码呼入的次数
var callin = hc.sql("select count(*) as callInCount,called_number as phoneNum from xjData group by called_number")
callin.registerTempTable("callin")


// 按号码合并呼入呼出次数,计算比值（这里用的join，可能会丢数据）,添加发现时间字段。注意：这里为了保证只有打出没有接入的号码，要用打出电话的号码集合left join打入的号码集合。因为使用了left join，在接入电话次数这个字段就可能出现null值，故需要对callInCount这个值进行null值判断，如果为null则设为1
var callOutIn = hc.sql("select callOut.phoneNum as sus_swindle_num, ceil(IF(callIn.callInCount is null,1,callIn.callInCount)/callout.callOutCount) as inDivOut,(callOut.callOutCount+IF(callIn.callInCount is null,1,callIn.callInCount)) as day_count,'"+ today +"' as find_time from callOut left join callIn on callIn.phoneNum=callOut.phoneNum")
callOutIn.registerTempTable("callOutIn")

// 根据嫌疑人诈骗规则过滤出嫌疑人
var swindlePersonAll = hc.sql("select coi.sus_swindle_num,i.city as phone_location,i.operator as phone_provider,coi.inDivOut,coi.day_count,coi.find_time from callOutIn as coi,ren.phoneindex as i,ren.swindle_rule as r where substr(coi.sus_swindle_num,-11,7)=i.section and coi.inDivOut<=ceil(r.in_dive_out/100) and coi.day_count>=r.day_count")
swindlePersonAll.registerTempTable("swindlePersonAll")

// 从疑似诈骗人中去除已经为诈骗人的数据
var swindleAlready = hc.sql("select swindlePersonAll.* from swindlePersonAll,ren.sblack where swindlePersonAll.sus_swindle_num=sblack.phoneNum")
var swindlePerson = swindlePersonAll.except(swindleAlready).show
