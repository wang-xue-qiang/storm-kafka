package com.zkh.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;


public class TridentTopo {

    public static StormTopology builder(LocalDRPC drpc)
    {
	    String zks = "hadoop-senior.ibeifeng.com:2181";
	    String topic = "area_order";
	    BrokerHosts brokerHosts = new ZkHosts(zks);
	    // 定义Trident拓扑，从Kafka中获取数据
        TridentKafkaConfig config = new TridentKafkaConfig(brokerHosts, topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(config);
        TridentTopology topology = new TridentTopology() ;
        //销售额
        //从Kafka获得输入
        TridentState amtState = topology.newStream("spout", spout)
        .parallelismHint(3)
        .each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderAmtSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
        .shuffle()
        .groupBy(new Fields("create_date","cf","province_id"))
        .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
        
        
        
        //提供查询销售额的功能，这里用来做测试
        topology.newDRPCStream("getOrderAmt", drpc)
        .each(new Fields("args"), new Split(" "), new Fields("arg"))
        .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
        .groupBy(new Fields("create_date","cf","province_id"))
        .stateQuery(amtState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("sum_amt"))
        .each(new Fields("sum_amt"),new FilterNull())
        .applyAssembly(new FirstN(5, "sum_amt", true));

        //订单数
        //从Kafka获得输入
/*        TridentState orderState = topology.newStream("orderSpout", spout)
        .parallelismHint(3)
        .each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderNumSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
        .shuffle()
        .groupBy(new Fields("create_date","cf","province_id"))
        .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_id"), new Count(), new Fields("order_num"));
      //提供查询订单数的功能，这里用来做测试
        topology.newDRPCStream("getOrderNum", drpc)
        .each(new Fields("args"), new Split(" "), new Fields("arg"))
        .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
        .groupBy(new Fields("create_date","cf","province_id"))
        .stateQuery(orderState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("order_num"))
        .each(new Fields("order_num"),new FilterNull());*/
        return topology.build() ;
    }
    /**
     * @param args
     * @throws AuthorizationException 
     */
    public static void main(String[] args) throws AuthorizationException {
        // TODO Auto-generated method stub
        LocalDRPC drpc = new LocalDRPC();
        Config conf = new Config() ;
        conf.setNumWorkers(5);
        conf.setDebug(false);
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder(null));
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder(drpc));
            while(true){
            	System.err.println(drpc.execute("getOrderAmt", "2018-10-11:1	2018-10-11:2	2018-10-11:3	2018-10-11:4	2018-10-11:5"));
            }
        }
    }

}
