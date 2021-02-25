package cn.syeet.theater.mqtt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.hutool.core.collection.CollectionUtil;

import cn.syeet.theater.config.DateProperties;
import cn.syeet.theater.config.IdConfig;
import cn.syeet.theater.entity.Device;
import cn.syeet.theater.entity.ErrorRecord;
import cn.syeet.theater.entity.Information;
import cn.syeet.theater.enumerate.GlobalCallbackEnum;
import cn.syeet.theater.exception.CustomErrorCodeEnum;
import cn.syeet.theater.exception.CustomException;
import cn.syeet.theater.model.bo.DeviceBO;
import cn.syeet.theater.model.bo.ErrorRecordBO;
import cn.syeet.theater.model.bo.InformationBO;
import cn.syeet.theater.service.ErrorRecordService;
import cn.syeet.theater.service.InformationService;
import cn.syeet.theater.util.BeanUtil;
import cn.syeet.theater.util.SpringUtil;
import cn.syeet.theater.websocket.WsServer;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.springframework.stereotype.Component;

/**
 * 使用autoWired注入获取不到service,可采用ApplicationContext获取 Spring是上下文的方式能够获得service服务
 * 
 * @author: xhl
 * @date: 2020年12月18日
 */
@Slf4j
@Component
public class PushCallback implements MqttCallback {

	private IdConfig bean;

	private DateProperties dateProperties;

	private List<Information> list = new ArrayList<>();

	private List<String> tagList = new ArrayList<>();

	@Override
	public void connectionLost(Throwable throwable) {
		log.error("Lost connection!!! {}");
		// ClientMqtt clientMqtt = new ClientMqtt();
		// try {
		// clientMqtt.run();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {

	}

	@Override
	public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
	//	 log.info("接收消息内容 : " + new String(mqttMessage.getPayload()));
		String data = new String(mqttMessage.getPayload());
		try {
			if ("/edge/add/465EA80EE6FB4F5E9A02B871DDFC329A/rtg".equals(topic)) {
				if (!"close".equals(data)) {
					JSONObject jsonObject = JSONObject.parseObject(data);
					if (jsonObject != null && jsonObject.containsKey("devs")) {
						String pKey = jsonObject.getString("pKey");
						JSONArray devsArr = jsonObject.getJSONArray("devs");
						if (devsArr != null && devsArr.size() > 0) {
							WsServer.sendMsg(pKey, devsArr.toString());
						}

						List<Information> list = getInformationList(devsArr);
						if (list.size() == 27) {
							log.info("list: "+list);
							insertList(list);
						}
						
//						List<ErrorRecord> errorRecordList = getErrorRecordList(devsArr);
//						if(CollectionUtil.isNotEmpty(errorRecordList)) {
//							insertErrList(errorRecordList);
//						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}



	
	public List<Information> getInformationList(JSONArray devsArr) {
		dateProperties = SpringUtil.getBean(DateProperties.class);
		List<String> dateList = dateProperties.getOpen();
	//	List<Information> list = new ArrayList<>();
		Date time = Calendar.getInstance().getTime();

		String minutes = Calendar.getInstance().get(Calendar.MINUTE) + "";
		boolean flag = dateList.contains(minutes);
		if (flag) {
			if (devsArr != null && devsArr.size() > 0) {
				for (int i = 0; i < devsArr.size(); i++) {
					JSONObject jsonObject2 = devsArr.getJSONObject(i);
					String dev = jsonObject2.getString("dev");
					if ("SR20".equals(dev)) {
						JSONArray jsonArray = jsonObject2.getJSONArray("d");
						if (jsonArray.size() > 1) {
							bean = SpringUtil.getBean(IdConfig.class);
							for (int j = 0; j < jsonArray.size(); j++) {
								JSONObject jsonObject3 = jsonArray.getJSONObject(j);
								InformationBO bo = new InformationBO();
								String m = jsonObject3.getString("m");
								String v = jsonObject3.getString("v");
								int tag = Integer.parseInt(m.substring(m.indexOf("_")+1, m.length()));
								switch (tag) {
								case 1:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-01");
									bo.setWarehouseNum(1);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 2:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-01");
									bo.setWarehouseNum(1);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 3:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-02");
									bo.setWarehouseNum(1);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 4:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-02");
									bo.setWarehouseNum(1);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 5:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-03");
									bo.setWarehouseNum(1);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 6:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-03");
									bo.setWarehouseNum(1);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 7:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-04");
									bo.setWarehouseNum(1);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 8:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-04");
									bo.setWarehouseNum(1);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 9:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-01");
									bo.setWarehouseNum(3);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 10:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-01");
									bo.setWarehouseNum(3);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 11:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-02");
									bo.setWarehouseNum(3);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 12:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-02");
									bo.setWarehouseNum(3);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 13:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-03");
									bo.setWarehouseNum(3);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 14:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-03");
									bo.setWarehouseNum(3);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 15:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-04");
									bo.setWarehouseNum(3);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 16:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-04");
									bo.setWarehouseNum(3);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 17:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-01");
									bo.setWarehouseNum(2);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 18:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-01");
									bo.setWarehouseNum(2);
									bo.setPointNum(1);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 19:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-02");
									bo.setWarehouseNum(2);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 20:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-02");
									bo.setWarehouseNum(2);
									bo.setPointNum(2);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 21:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-03");
									bo.setWarehouseNum(2);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 22:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-03");
									bo.setWarehouseNum(2);
									bo.setPointNum(3);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 23:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-04");
									bo.setWarehouseNum(2);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(0);
									bo.setCreateDate(time);
									break;
								case 24:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-04");
									bo.setWarehouseNum(2);
									bo.setPointNum(4);
									bo.setValue(Double.valueOf(v));
									bo.setType(1);
									bo.setCreateDate(time);
									break;
								case 25:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("1号-VOC");
									bo.setWarehouseNum(1);
									bo.setPointNum(0);
									bo.setValue(Double.valueOf(v));
									bo.setType(2);
									bo.setCreateDate(time);
									break;
								case 26:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("3号-VOC");
									bo.setWarehouseNum(3);
									bo.setPointNum(0);
									bo.setValue(Double.valueOf(v));
									bo.setType(2);
									bo.setCreateDate(time);
									break;
								case 27:
									bo.setId(bean.snowflake().nextId());
									bo.setName(m);
									bo.setAddr("2号-VOC");
									bo.setWarehouseNum(2);
									bo.setPointNum(0);
									bo.setValue(Double.valueOf(v));
									bo.setType(2);
									bo.setCreateDate(time);
									break;
								}
								if ("40".equals(minutes)) {
									SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH");
									String format = sdf.format(time);
									bo.setHours(format);
								}
								Information information = BeanUtil.copy(bo, Information.class);
								if(list.size() < 27) {
									list.add(information);	
								}
														
							}
							
						}
					}
				}
			}
		} else if (CollectionUtil.isNotEmpty(list)) {
			list.clear();
		}

		return list;

	}

	public List<ErrorRecord> getErrorRecordList(JSONArray devsArr) {
		List<ErrorRecord> errList = new ArrayList<>();
		if (devsArr != null && devsArr.size() > 0) {
			for (int i = 0; i < devsArr.size(); i++) {
				JSONObject jsonObject2 = devsArr.getJSONObject(i);
				String dev = jsonObject2.getString("dev");
				if ("SR30".equals(dev)) {
					JSONArray jsonArray = jsonObject2.getJSONArray("d");
					if (jsonArray.size() > 1) {
						bean = SpringUtil.getBean(IdConfig.class);
						for (int j = 0; j < jsonArray.size(); j++) {
							JSONObject jsonObject3 = jsonArray.getJSONObject(j);
							ErrorRecordBO bo = new ErrorRecordBO();
							String m = jsonObject3.getString("m");
							String v = jsonObject3.getString("v");
							int tag = Integer.parseInt(m.substring(m.indexOf("_")+1, m.length()));
							switch (tag) {
							case 28:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("机组");
										bo.setErrorDescription("机组故障反馈");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 29:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("机组运行");
										bo.setErrorDescription("机组运行反馈报警");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 30:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("送风机");
										bo.setErrorDescription("送风机故障");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 31:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("失风机");
										bo.setErrorDescription("失风故障");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 32:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("加湿器");
										bo.setErrorDescription("加湿器故障");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 33:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("温湿度传感器");
										bo.setErrorDescription("温湿度传感器故障");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 34:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("防冻保护");
										bo.setErrorDescription("防冻保护报警");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;
								
							case 35:
								if ("true".equals(v)) {
									if (!(tagList.contains(m))) {
										bo.setId(bean.snowflake().nextId());
										bo.setDeviceName("消防报警");
										bo.setErrorDescription("消防报警");
										bo.setCreateDate(Calendar.getInstance().getTime());
										tagList.add(m);
									}
								} else if (tagList.contains(m)) {
									tagList.removeIf(e -> e.equals(m));
								}
								break;	

							}
							ErrorRecord copy = BeanUtil.copy(bo, ErrorRecord.class);
							if(StringUtils.isNotBlank(copy.getDeviceName())) {
								errList.add(copy);
							}
													
						}
					}
				}
			}
		}
		return errList;

	}


	public void insertList(List<Information> list) {
		InformationService serviceBean = SpringUtil.getBean(InformationService.class);
			if (!(serviceBean.saveBatch(list))) {
				throw new CustomException(GlobalCallbackEnum.SQL_INSERT_FAILURE);
		}

	}
		
	public void insertErrList(List<ErrorRecord> errorRecordList) {
		ErrorRecordService errorRecordService = SpringUtil.getBean(ErrorRecordService.class);
		if (!(errorRecordService.saveBatch(errorRecordList))) {
			throw new CustomException(GlobalCallbackEnum.SQL_INSERT_FAILURE);
		}
	}

}
