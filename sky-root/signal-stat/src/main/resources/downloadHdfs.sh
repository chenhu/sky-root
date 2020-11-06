#!/bin/bash
#根据输入的地市编码下载数据

if [ ! -n "$1" ]
then
    echo -e "请输入地市编码"
    exit -1
fi
cityCode=$1

#统计表格名称表格
little="day-trip-summary-stat day-trip-with-purpose-summary-stat migrate-stat od-time-distance-stat"
big="base-hour od-day-stat od-time-interval-general-stat"
profiles="2017 2019mainstat 2019otherstat"
workliveProfile="201706worklivestat 201906worklivestat"

basepath="/user/bdoc/17/services/hdfs/132/jiangsu_track_second/stat"
dataDir="$HOME/jiangsu/data1"
echo -e "current data dir is:$dataDir"
#下载worklive
for profile in $workliveProfile;
do
    hdfsPath="$basepath/$profile/$cityCode/work-live-stat"
    statdir="$dataDir/$cityCode/$profile"
    mkdir -p $statdir
    echo -e "+++++++++++++++++++++++++++++++"
    echo -e "Downloading $hdfsPath to $statdir ..."
    hdfs dfs -get $hdfsPath $statdir
    echo -e "Download done!"

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
        echo -e "+++++++++++++++++++++++++++++++"
        echo -e "Downloading $hdfsPath to $statdir ..."
        hdfs dfs -get $hdfsPath $statdir
        echo -e "Download done!"
        #TEST
        # mkdir -p $statdir/$table
        # echo "test" >> $statdir/$table/part-00000
    done;
done;

#合并表格内容
statdir="$dataDir/$cityCode"
mkdir -p $statdir/stat
#download geohash
hdfs dfs -get /user/bdoc/17/services/hdfs/132/jiangsu_track_second/save/$cityCode/geohash $statdir/stat/
#download dataquality
hdfs dfs -get /user/bdoc/17/services/hdfs/132/jiangsu_track_second/save/$cityCode/quality-all $statdir/stat/

#合并worklive
echo -e "merge worklive files...."
for profile in $workliveProfile;
do
    localdir="$dataDir/$cityCode/$profile/work-live-stat"
    destdir="$statdir/stat/work-live-stat"
    mkdir -p $destdir
    for file in $(ls $localdir)
    do
        if [ ! -f "$destdir/$file" ]
        then
            mv $localdir/$file $destdir/$file
        elif [ ! -f "$destdir/${file}_1" ]
        then
            mv $localdir/$file $destdir/${file}_1
        else
            mv $localdir/$file $destdir/${file}_2
        fi
    done;
done;
#合并其他表格
echo -e "merge other tables..."
for profile in $profiles;
do
    for table in $little;
    do
        localdir="$dataDir/$cityCode/$profile/$table"
        destdir="$statdir/stat/$table"
        mkdir -p $destdir
        for file in $(ls $localdir)
        do
            if [ ! -f "$destdir/$file" ]
            then
                mv $localdir/$file $destdir/$file
            elif [ ! -f "$destdir/${file}_1" ]
            then
                mv $localdir/$file $destdir/${file}_1
             else
                mv $localdir/$file $destdir/${file}_2
             fi
        done;

    done;
done;
#clear 
echo -e "Clearing tmp dirs..."
for profile in $profiles;
do
  rm -rf $dataDir/$cityCode/$profile
done;
for profile in $workliveProfile;
do
  rm -rf $dataDir/$cityCode/$profile
done;

echo -e "Download data from hdfs for city $cityCode finished!"

