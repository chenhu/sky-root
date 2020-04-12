淮安统计表和类对应表-cmcc2019001
-----------------------------------------------------------------------------------------------------------------------
|   类名                   |   需求表名                                      | 统计报名路径名
-----------------------------------------------------------------------------------------------------------------------
DataQualityProcessor        表1 数据特征                                         stat/dataquality
BaseHourStat                表2 区域每1小时人口统计特征                             stat/base-hour
WorkLiveStat                表3 人口居住地及就业地统计表                             stat/work-live-stat
*ODDayStat                   表4 基站日OD表                                       stat/od-day-stat
*DayTripSummaryStat          表5 日出行总体特征                                     stat/day-trip-summary-stat
*DayTripPurposeSummaryStat   表6 分目的的总体特征                                   stat/day-trip-with-purpose-summary-stat
*ODTimeIntervalStat          表7 基站特定时间间隔（1小时）OD表                        stat/od-time-interval-general-stat
*ODTimeIntervalStat          表7-1基站特定时间间隔（高峰时刻）OD表                     stat/od-time-interval-special-stat
*ODTimeIntervalStat          表8 基站特定时间间隔（1小时）分目的出行特征统计             stat/od-time-interval-purpose-general-stat
*ODTimeIntervalStat          表8-1基站特定时间间隔（高峰平峰时刻）分目的出行特征统计       stat/od-time-interval-purpose-special-stat
*ODTimeDistanceStat          表9 odtimestance出行时耗-距离分布表                     stat/od-time-distance-stat
ODTraceStat                 表10 全日出行轨迹表                                    stat/od-trace-day
ODTraceStat                 表11 高峰出行轨迹表                                    stat/od-trace-busy-time
*ODTripStat                  表12 日出行人口情况                                    stat/od-trip-class-stat
ValidSignalStat             表12-1 日出行人口情况                                   stat/valid-stat

深圳项目分析南京手机信令对应的报表-cmcc2019003


常熟项目分析常熟手机信令对应的报表-cmcc2020002
完成 WorkLiveStat              表1-1 人口分布集合数据表                             stat/work-live-stat           workLiveStatService
完成 baseHourStat              表1-2 人口活动热力分布表                             stat/base-hour                baseHourStatService
完成 odDayStat                 表2-1 日内部人口流动数据表                            stat/od-day-stat            oDDayStatService
完成 dayTripSummaryStat        表2-2 日总体出行特征                                 stat/day-trip-summary-stat   oDTripStatService
完成 dayTripPurposeSummaryStat 表2-3 分目的总体出行特征                              stat/day-trip-with-purpose-summary-stat  oDTripStatService
完成 oDTimeIntervalStat        表2-4 网格特定时间间隔（半小时）OD表                   stat/od-time-interval-general-stat  oDTimeStatService
完成 oDTimeDistanceStat        表2-5 odtimestance出行时耗-距离分布表                 stat/od-time-distance-stat  oDTimeStatService



