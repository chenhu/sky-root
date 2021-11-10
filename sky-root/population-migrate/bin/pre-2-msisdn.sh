service="--service=msisdnCollectionService"
spec_param="--day=20190204,20190612,20190616,20191002 --partitions=1200"
command="spark-submit $param  $service $spec_param $region" 
$command
