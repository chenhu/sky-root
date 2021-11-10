#!/bin/bash
home=/home/xxzx_user/population-jiangsu
file="signal-pre-1.0-SNAPSHOT.jar"
app=$home/jars/pre/$file
source $home/conf/common
source $home/conf/cityConf
echo -e $param
echo -e "pre process starting ..."
#exec script
source $home/bin/pre-2-0-msisdn-stat.sh
#source $home/bin/pre-1-cell.sh && source $home/bin/pre-2-0-msisdn-stat.sh && source $home/bin/pre-2-msisdn.sh && source $home/bin/pre-2-filter-trace.sh &&
# source $home/bin/pre-3-signal.sh && source $home/bin/pre-4-odService.sh
