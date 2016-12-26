import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyyMMdd")
var cal = Calendar.getInstance()
var today = dateformat.format(cal.getTime())

var hc = new org.apache.spark.sql.hive.HiveContext(sc)
//  �����½������ݣ�Ϊ�˱���ʹ��or�������õ���union all��where�������жϺ������������һ��Ϊ�½������ݡ�ע������ĺ���α��õ���phoneindex��Ӧ���޸�Ϊxinjiang_phoneindex
var xjData = hc.sql("select from_unixtime(ceil(i.btime/1000),'yyyy-MM-dd-HH') as time_Stamp,i.calling_number,i.called_number from ren.bicc_isup_less as i,ren.phoneindex as h where substr(i.calling_number,-11,7)=h.section union all select from_unixtime(ceil(i.btime/1000),'yyyy-MM-dd-HH') as time_Stamp,i.calling_number,i.called_number from ren.bicc_isup_less as i,ren.phoneindex as h where substr(i.called_number,-11,7)=h.section ")
xjData.registerTempTable("xjData")

// ͳ��ÿ����������Ĵ���
var callout = hc.sql("select count(*) as callOutCount,calling_number as phoneNum from xjData group by calling_number")
callout.registerTempTable("callout")

// ͳ��ÿ���������Ĵ���
var callin = hc.sql("select count(*) as callInCount,called_number as phoneNum from xjData group by called_number")
callin.registerTempTable("callin")


// ������ϲ������������,�����ֵ�������õ�join�����ܻᶪ���ݣ�,��ӷ���ʱ���ֶΡ�ע�⣺����Ϊ�˱�ֻ֤�д��û�н���ĺ��룬Ҫ�ô���绰�ĺ��뼯��left join����ĺ��뼯�ϡ���Ϊʹ����left join���ڽ���绰��������ֶξͿ��ܳ���nullֵ������Ҫ��callInCount���ֵ����nullֵ�жϣ����Ϊnull����Ϊ1
var callOutIn = hc.sql("select callOut.phoneNum as sus_swindle_num, ceil(IF(callIn.callInCount is null,1,callIn.callInCount)/callout.callOutCount) as inDivOut,(callOut.callOutCount+IF(callIn.callInCount is null,1,callIn.callInCount)) as day_count,'"+ today +"' as find_time from callOut left join callIn on callIn.phoneNum=callOut.phoneNum")
callOutIn.registerTempTable("callOutIn")

// ����������թƭ������˳�������
var swindlePersonAll = hc.sql("select coi.sus_swindle_num,i.city as phone_location,i.operator as phone_provider,coi.inDivOut,coi.day_count,coi.find_time from callOutIn as coi,ren.phoneindex as i,ren.swindle_rule as r where substr(coi.sus_swindle_num,-11,7)=i.section and coi.inDivOut<=ceil(r.in_dive_out/100) and coi.day_count>=r.day_count")
swindlePersonAll.registerTempTable("swindlePersonAll")

// ������թƭ����ȥ���Ѿ�Ϊթƭ�˵�����
var swindleAlready = hc.sql("select swindlePersonAll.* from swindlePersonAll,ren.sblack where swindlePersonAll.sus_swindle_num=sblack.phoneNum")
var swindlePerson = swindlePersonAll.except(swindleAlready).show
