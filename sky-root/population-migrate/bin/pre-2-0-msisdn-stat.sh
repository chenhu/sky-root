#统计指定日期、地市、区县的手机号码数量
service="--service=msisdnStatService"
spec_param="--day=20190610 --partitions=1200"
command="spark-submit $param  $service $spec_param $region" 
$command
