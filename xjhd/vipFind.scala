import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyyMMdd")
var cal = Calendar.getInstance()
var today = dateformat.format(cal.getTime())

var hc = new org.apache.spark.sql.hive.HiveContext(sc)
// 过滤xj与国外通话的数据。即通话一方为xj，另一方号码开头为00
// 并增加xj号码字段和国外号码字段
// 下面语句没有加时间范围条件，这里要加近一周，分析近一周的数据
// 去除vip的通话数据。如果包含vip的数据，有可能挖掘到的ysr本来就是vip
// 这里去除的方法为：先取全集，再从全集中去除是vip的通话数据。这么做的原因是，在hive sql中不能用“字段A!=字段B”的where条件，“=”其实是join条件的缩写，如果用“!=”其实是指join条件为不等于
var xj2FDataAll = hc.sql("select i.calling_number,i.called_number,i.calling_number as xj_phone,i.called_number as f_phone from ren.bicc_isup_less as i,ren.xjindex as h where substr(i.calling_number,-11,7)=h.section and substr(i.called_number,0,2)='00' union all select i.calling_number,i.called_number,i.called_number as xj_phone,i.calling_number as f_phone from ren.bicc_isup_less as i,ren.xjindex as h where substr(i.called_number,-11,7)=h.section and substr(i.calling_number,0,2)='00'")
xj2FDataAll.registerTempTable("xj2FDataAll")

// vip的数据，即呼入为vip号 + 呼出为vip号
var xj2FDataVip = hc.sql("select xjAll.* from xj2FDataAll as xjAll,ren.vipblack as vb where xjAll.calling_number=vb.phoneNum union all select xjAll.* from xj2FDataAll as xjAll,ren.vipblack as vb where xjAll.called_number=vb.phoneNum")
xj2FDataVip.registerTempTable("xj2FDataVip")

// 从全集中去除vip数据
var xj2FData=xj2FDataAll.except(xj2FDataVip)
xj2FData.registerTempTable("xj2FData")

// 过滤与关注国家的通话，并按xj号码分组统计。注意：当前没有统按国家分别进行统计
var xjSus = hc.sql("select first(xj.xj_phone) as xj_phone,first(xj.f_phone) as f_phone,count(*) as com_count from xj2FData as xj,ren.special_country as c where substr(xj.f_phone,3,4)=c.code or substr(xj.f_phone,3,3)=c.code or substr(xj.f_phone,3,2)=c.code or substr(xj.f_phone,3,1)=c.code group by xj.xj_phone order by com_count desc limit 100")
xjSus.registerTempTable("xjSus")


// 从原始数据中获取ysr与vip的通话数据
var sus2vipData = hc.sql("select bi.calling_number,bi.called_number,xjs.xj_phone,xjs.f_phone,xjs.com_count,vb.phoneNum as vip_black_phone from ren.bicc_isup_less as bi,xjSus as xjs,ren.vipblack as vb where bi.calling_number=xjs.xj_phone and bi.called_number=vb.phoneNum union all select bi.calling_number,bi.called_number,xjs.xj_phone,xjs.f_phone,xjs.com_count,vb.phoneNum as vip_black_phone from ren.bicc_isup_less as bi,xjSus as xjs,ren.vipblack as vb where bi.called_number=xjs.xj_phone and bi.calling_number=vb.phoneNum")
sus2vipData.registerTempTable("sus2vipData")

// 将ysr与vip通话数据，按ysr分组统计通话次数并排序,获取前20个为ysr
// 注意，此时可能不够20个，即与vip有联系的ysr足20个，所以要从原来的ysr中补齐20个
var comVipSus = hc.sql("select first(xj_phone) as xj_phone,first(f_phone) as f_phone,first(com_count) as com_count,count(*) as com2vip_count from sus2vipData group by xj_phone order by com2vip_count desc limit 20")

comVipSus.registerTempTable("comVipSus")

var xjSusAndComVip = hc.sql("select xjSus.*,IF(comVipSus.com2vip_count is null,0,comVipSus.com2vip_count) as com2vip_count from xjSus left join comVipSus on xjSus.xj_phone=comVipSus.xj_phone order by com2vip_count desc,com_count desc limit 20")
xjSusAndComVip.registerTempTable("xjSusAndComVip")

// 生成ysvip，最频繁国家暂时为空(全设为国外)，最频繁comm的vip为空
var vip_suspicious = hc.sql("select vb.xj_phone as sus_vip_num,i.city as phone_location,i.operator as phone_provider,'国外' as frequency_country,null as frequency_vip,'"+ today +"' as find_time from xjSusAndComVip as vb,ren.xjindex as i where substr(vb.xj_phone,-11,7)=i.section").show

