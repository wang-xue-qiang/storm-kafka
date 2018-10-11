package com.zkh.util;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
public class DateUtils {
	/**
	 * 计算日期
	 * @param date    日期字符串
	 * @param pattern 日期格式化
	 * @param step    计算量
	 * @return
	 */
	public static String getDate(String date,String pattern,int step){
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar cal  = Calendar.getInstance();
		if(date != null){
			try {
				cal.setTime(sdf.parse(date));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		cal.add(Calendar.DATE, step);
		return sdf.format(cal.getTime());
	}
	
	
	public static void main(String[] args) {
		System.out.println(DateUtils.getDate(null, "yyyy-MM-dd", -1));
	}
}
