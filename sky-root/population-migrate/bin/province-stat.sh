service="--service=provinceOdStatService"
spec_param=" --partitions=1000"
command="spark-submit $param  $service $spec_param $region" 
$command
