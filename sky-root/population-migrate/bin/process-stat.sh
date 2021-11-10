#!/bin/bash
home=/home/xxzx_user/population-jiangsu
file="population-migrate-1.0-SNAPSHOT.jar"
app=$home/jars/stat/$file
echo -e "current app path is: $app"
source $home/conf/common-population
source $home/conf/cityConf
echo -e $param

#exec script
source $home/bin/province-odService.sh && $home/bin/province-stat.sh
