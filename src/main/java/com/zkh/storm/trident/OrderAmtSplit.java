package com.zkh.storm.trident;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
public class OrderAmtSplit extends BaseFunction{
	private static final long serialVersionUID = 1L;
	String patten = null;
	public OrderAmtSplit(String patten){
		this.patten = patten;		
	}
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.out.println("=========>"+tuple.toString());
		if(!tuple.isEmpty()){
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten);
			collector.emit(new Values(value[0],value[1],value[2],value[3]));
			
		}
		
	}

	

}
