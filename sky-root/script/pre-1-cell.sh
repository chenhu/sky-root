. ~/jiangsu/common
service="--service=CellService"
spec_param="--partitions=20"
command="spark-submit $param  $service $spec_param" 
$command
