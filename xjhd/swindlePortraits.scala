var hc = new org.apache.spark.sql.hive.HiveContext(sc)
//  过滤zp电话的数据
var balckPhone = hc.sql("select from_unixtime(ceil(i.btime/1000),'yyyy-MM-dd-HH') as time_Stamp,i.calling_number,i.called_number,i.answerdur,b.phonenum as swindle_Num from ren.bicc_isup as i,ren.sblack as b where substr(i.calling_number,-11)=b.phonenum or substr(i.called_number,-11)=b.phonenum")
balckPhone.registerTempTable("balckPhone")

// 呼入呼出为手机号
var callingPhone = hc.sql("select i.*,p.province as calling_province  from balckPhone as i,ren.phoneindex as p where substr(i.calling_number,-11,7)=p.section")
callingPhone.registerTempTable("callingPhone")

var allPhone = hc.sql("select i.*,p.province as called_province from callingPhone as i,ren.phoneindex as p where substr(i.called_number,-11,7)=p.section")
allPhone.registerTempTable("allPhone")

var xjCallData = hc.sql("select * from allPhone where allPhone.calling_province='河南' or allPhone.called_province='河南'")
xjCallData.registerTempTable("xjCallData")

var modelTB = hc.sql("select *,IF(calling_province='河南',called_province,calling_province) as city from xjCallData")
modelTB.registerTempTable("modelTB")

// zp号码画像表
var cityTotal = hc.sql("select swindle_Num,time_Stamp,city,count(*) as call_city_count,sum(IF(called_province='河南',1,0)) as call_in_city_count,sum(IF(calling_province='河南',1,0)) as call_out_city_count,sum(answerdur) as call_city_time,sum(IF(called_province='河南',answerdur,0)) as call_in_city_time,sum(IF(calling_province='河南',answerdur,0)) as call_out_city_time,sum(IF(ceil(answerdur/1000)<=60,1,0)) as call_duration1,sum(IF(ceil(answerdur/1000)>60 and ceil(answerdur/1000)<=300,1,0)) as call_duration5,sum(IF(ceil(answerdur/1000)>300,1,0)) as call_duration5plus from modelTB group by swindle_Num,time_Stamp,city")
cityTotal.registerTempTable("cityTotal")
