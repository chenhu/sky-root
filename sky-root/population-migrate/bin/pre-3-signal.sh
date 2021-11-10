service="--service=signalProcessService"
city_param=" --citys=$jscity "
spec_param="--day=20190204,20190612,20190616,20191002 --partitions=4000"
command="spark-submit $param  $service $spec_param $region  $city_param" 
$command
