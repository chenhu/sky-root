#!/bin/bash
#根据输入的地市编码下载数据

if [ ! -n "$1" ]
then
    echo -e "请输入地市编码"
    exit 1
fi
cityCode=$1

#统计表格名称表格
little="day-trip-summary-stat day-trip-with-purpose-summary-stat migrate-stat od-time-distance-stat"
big="base-hour od-day-stat od-time-interval-general-stat"
profiles="2017 2019mainstat 2019otherstat"
workliveProfile="201706worklivestat 201906worklivestat"

basepath="/user/bdoc/17/services/hdfs/132/jiangsu_track_second/stat"
dataDir="$HOME/jiangsu/data"
#下载worklive
for profile in $workliveProfile;
do
    hdfsPath="$basepath/$profile/$cityCode/work-live-stat"
    statdir="$dataDir/$cityCode/$profile"
    mkdir -p $statdir
    hdfs dfs -get $hdfsPath $statdir
    #TEST
    # mkdir -p $statdir/work-live-stat
    # echo "test" >> $statdir/work-live-stat/part-00000
done;

#下载表格3～9
for profile in $profiles;
do
    for table in $little;
    do
        hdfsPath="$basepath/$profile/$cityCode/$table"
        statdir="$dataDir/$cityCode/$profile"
        mkdir -p $statdir
        hdfs dfs -get $hdfsPath $statdir
        #TEST
        # mkdir -p $statdir/$table
        # echo "test" >> $statdir/$table/part-00000
    done;
done;

#合并表格内容
statdir="$dataDir/$cityCode"
mkdir -p $statdir/stat
#合并worklive
for profile in $workliveProfile;
do
    localdir="$dataDir/$cityCode/$profile"
    destdir="$statdir/stat/"
    mv $localdir $destdir
done;
#合并其他表格
for profile in $profiles;
do
    for table in $little;
    do
        localdir="$dataDir/$cityCode/$profile/$table"
        destdir="$statdir/stat/$table"
        mkdir -p $destdir
        for file in $(ls $localdir)
        do
            echo $file
            if [ ! -f "$destdir/$file" ]
            then
                mv $localdir/$file $destdir/$file
            else
                mv $localdir/$file $destdir/${file}_1
            fi
        done;

    done;
done;




