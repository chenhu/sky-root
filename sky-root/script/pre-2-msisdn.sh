. ~/jiangsu/common
. ~/jiangsu/cityConf
service="--service=msisdnCollectionService"
spec_param="--day=20190612,20190616,20191002,20190204 --partitions=6000"
command="spark-submit $param  $service $spec_param $region" 
$command
