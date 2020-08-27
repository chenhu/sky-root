. ~/jiangsu/common
. ~/jiangsu/cityConf
service="--service=odService"
spec_param="--day=20190612,20190616,20191002 --partitions=9000"
command="spark-submit $param  $service $spec_param $region" 
$command
