. ~/jiangsu/common_population
. ~/jiangsu/cityConf
service="--service=districtOdStatService"
spec_param=" --partitions=10"
command="spark-submit $param  $service $spec_param $region" 
$command
