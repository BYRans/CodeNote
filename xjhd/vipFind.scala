import java.text.SimpleDateFormat
import java.util.Calendar
var dateformat = new SimpleDateFormat("yyyyMMdd")
var cal = Calendar.getInstance()
var today = dateformat.format(cal.getTime())

var hc = new org.apache.spark.sql.hive.HiveContext(sc)
// �����½������ͨ�������ݡ���ͨ��һ��Ϊ�½������أ���һ�����뿪ͷΪ00
// �������½������ֶκ͹�������ֶ�
// �������û�м�ʱ�䷶Χ����������Ҫ�ӽ�һ�ܣ�������һ�ܵ�����
// ȥ���ص��˵�ͨ�����ݡ���������ص��˵����ݣ��п����ھ򵽵������˱��������ص���
// ����ȥ���ķ���Ϊ����ȡȫ�����ٴ�ȫ����ȥ�����ص��˵�ͨ�����ݡ���ô����ԭ���ǣ���hive sql�в����á��ֶ�A!=�ֶ�B����where��������=����ʵ��join��������д������á�!=����ʵ��ָjoin����Ϊ������
var xj2FDataAll = hc.sql("select i.calling_number,i.called_number,i.calling_number as xj_phone,i.called_number as f_phone from ren.bicc_isup_less as i,ren.xjindex as h where substr(i.calling_number,-11,7)=h.section and substr(i.called_number,0,2)='00' union all select i.calling_number,i.called_number,i.called_number as xj_phone,i.calling_number as f_phone from ren.bicc_isup_less as i,ren.xjindex as h where substr(i.called_number,-11,7)=h.section and substr(i.calling_number,0,2)='00'")
xj2FDataAll.registerTempTable("xj2FDataAll")

// �ص��˵����ݣ�������Ϊ�ص��˺� + ����Ϊ�ص��˺�
var xj2FDataVip = hc.sql("select xjAll.* from xj2FDataAll as xjAll,ren.vipblack as vb where xjAll.calling_number=vb.phoneNum union all select xjAll.* from xj2FDataAll as xjAll,ren.vipblack as vb where xjAll.called_number=vb.phoneNum")
xj2FDataVip.registerTempTable("xj2FDataVip")

// ��ȫ����ȥ���ص�����������
var xj2FData=xj2FDataAll.except(xj2FDataVip)
xj2FData.registerTempTable("xj2FData")

// �������ע���ҵ�ͨ���������½��������ͳ�ơ�ע�⣺��ǰû��ͳ�����ҷֱ����ͳ��
var xjSus = hc.sql("select first(xj.xj_phone) as xj_phone,first(xj.f_phone) as f_phone,count(*) as com_count from xj2FData as xj,ren.special_country as c where substr(xj.f_phone,3,4)=c.code or substr(xj.f_phone,3,3)=c.code or substr(xj.f_phone,3,2)=c.code or substr(xj.f_phone,3,1)=c.code group by xj.xj_phone order by com_count desc limit 100")
xjSus.registerTempTable("xjSus")


// ��ԭʼ�����л�ȡ���������ص��˵�ͨ������
var sus2vipData = hc.sql("select bi.calling_number,bi.called_number,xjs.xj_phone,xjs.f_phone,xjs.com_count,vb.phoneNum as vip_black_phone from ren.bicc_isup_less as bi,xjSus as xjs,ren.vipblack as vb where bi.calling_number=xjs.xj_phone and bi.called_number=vb.phoneNum union all select bi.calling_number,bi.called_number,xjs.xj_phone,xjs.f_phone,xjs.com_count,vb.phoneNum as vip_black_phone from ren.bicc_isup_less as bi,xjSus as xjs,ren.vipblack as vb where bi.called_number=xjs.xj_phone and bi.calling_number=vb.phoneNum")
sus2vipData.registerTempTable("sus2vipData")

// �����������ص���ͨ�����ݣ��������˷���ͳ��ͨ������������,��ȡǰ20��Ϊ������
// ע�⣬��ʱ���ܲ���20���������ص�������ϵ�������˲���20��������Ҫ��ԭ�����������в���20��
var comVipSus = hc.sql("select first(xj_phone) as xj_phone,first(f_phone) as f_phone,first(com_count) as com_count,count(*) as com2vip_count from sus2vipData group by xj_phone order by com2vip_count desc limit 20")

comVipSus.registerTempTable("comVipSus")

var xjSusAndComVip = hc.sql("select xjSus.*,IF(comVipSus.com2vip_count is null,0,comVipSus.com2vip_count) as com2vip_count from xjSus left join comVipSus on xjSus.xj_phone=comVipSus.xj_phone order by com2vip_count desc,com_count desc limit 20")
xjSusAndComVip.registerTempTable("xjSusAndComVip")

// ���������ص��˱���Ƶ��������ʱΪ��(ȫ��Ϊ����)����Ƶ����ϵ���ص���Ϊ��
var vip_suspicious = hc.sql("select vb.xj_phone as sus_vip_num,i.city as phone_location,i.operator as phone_provider,'����' as frequency_country,null as frequency_vip,'"+ today +"' as find_time from xjSusAndComVip as vb,ren.xjindex as i where substr(vb.xj_phone,-11,7)=i.section").show

