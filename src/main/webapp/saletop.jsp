<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title></title>
<script src="https://img.hcharts.cn/highcharts/highcharts.js"></script>
<script src="https://img.hcharts.cn/highcharts/modules/exporting.js"></script>
<script src="https://img.hcharts.cn/highcharts-plugins/highcharts-zh_CN.js"></script>
<script type="text/javascript">

</script>
</head>
<body>
<div id="container" style="height: 600px;width: 100%"></div>
<script type="text/javascript">
var chart = Highcharts.chart('container', {
    chart: {
        zoomType: 'xy'
    },
    title: {
        text: '区域订单金额和订单数量统计'
    },
    subtitle: {
        text: ''
    },
	plotOptions: {
		column: {
			dataLabels:{
				enabled:true
			}
		}
	},
    xAxis: [{
    	categories:['北京','上海','广州','深圳','郑州'],
        crosshair: true
    }],
    yAxis: [{ // Primary yAxis
        labels: {
            format: '{value}',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        },
        title: {
            text: '订单数',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        }
    }, { // Secondary yAxis
        title: {
            text: '销售额',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        },
        labels: {
            format: '{value}',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        },
        opposite: true
    }],
    tooltip: {
        shared: true
    },
    legend: {
        layout: 'vertical',
        align: 'left',
        x: 120,
        verticalAlign: 'top',
        y: 100,
        floating: true,
        backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
    },
    series: [{
        name: '销售额',
        type: 'column',
        yAxis: 1,
        data: [49.9, 71.5, 106.4, 129.2, 144.0],
        tooltip: {
            valueSuffix: ' 元'
        }
    }, {
        name: '订单数',
        type: 'spline',
        data: [7.0, 6.9, 9.5, 14.5, 18.2],
        tooltip: {
            valueSuffix: '个'
        }
    }]
});
</script>
</body>
</html>