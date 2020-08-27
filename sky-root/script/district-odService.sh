. ~/jiangsu/common_population
. ~/jiangsu/cityConf
service="--service=districtOdService"
spec_param="--day=20191002 --partitions=1800"
command="spark-submit $param  $service $spec_param $region" 
$command
