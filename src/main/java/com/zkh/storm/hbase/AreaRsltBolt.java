package com.zkh.storm.hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.zkh.hbase.dao.HbaseDao;
import com.zkh.hbase.dao.impl.HbaseDaoImpl;

public class AreaRsltBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	Map<String ,Double> countsMap = null;
	HbaseDao hbaseDao = null;
	long beginTime = System.currentTimeMillis();
	long endTime = 0l;
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		countsMap = new HashMap<String ,Double>();
		hbaseDao = new HbaseDaoImpl();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(input !=null){
			String date_area = (String)input.getValueByField("date_area");
			System.err.println("AreaRsltBolt===========>"+input.toString()+"///"+input.getDoubleByField("amt"));
			countsMap.put(date_area, input.getDoubleByField("amt"));
			//没5秒计算一次
			endTime = System.currentTimeMillis();
			if(endTime- beginTime >= 5*1000){
				for(String key:countsMap.keySet()){
					//存入hbase:格式 2018-10-10,amt
					hbaseDao.insert("area_order", key, "cf", "order_amt", countsMap.get(key)+"");
				}
				beginTime = System.currentTimeMillis();
			}			
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		countsMap.clear();
	}

}
