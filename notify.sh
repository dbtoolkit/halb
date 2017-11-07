#!/bin/bash

type=$1
name=$2
targetstate=$3

currDate=$(date '+%Y%m%d')
currDatetime=$(date '+%Y-%m-%d %H:%M:%S')
notifyLog=/var/log/hanotify.log
haConf=/etc/keepalived/haconf
halbLog=/var/log/halb_${name}.log

localIp=`hostname -i`

exec 3>&1 4>&2 1>>$notifyLog 2>&1
echo "$currDatetime $@"

# 检查vrrpinstance是否在高可用全局配置文件haconf中，若不存在，则直接退出切换
/bin/awk '{IGNORECASE=1; if($8=="'$name'") print $1}' $haConf|grep -v "^#"|tr '[:lower:]' '[:upper:]' >/tmp/haconf.tmp
if [ ! -s /tmp/haconf.tmp ]; then
   echo "`date '+%Y-%m-%d %H:%M:%S'` Error $localIp vrrp_instance $name not in haconf" >> $halbLog
   exit 0
fi

# 检查高可用全局配置文件haconf，若高可用设置为“维护模式”，则直接退出切换 
while read IsMaintainance
do
  if [ "$IsMaintainance" == "Y" ]; then
    echo "`date '+%Y-%m-%d %H:%M:%S'` Warning $localIp vrrp_instance: $name is in maintainance mode" >> $halbLog
    exit 0
  fi
done < /tmp/haconf.tmp

# 根据keepalived状态，调用高可用切换脚本
if [ "$type" == "INSTANCE" ]; then
  if [ "$targetstate" == "MASTER" ]; then
    /etc/keepalived/promote.pl --type=$type --vrrpinstance=$name --state=$targetstate
  elif [ "$targetstate" == "BACKUP" ]; then
    /etc/keepalived/demote.pl --type=$type --vrrpinstance=$name --state=$targetstate
  fi
fi

exec 1>&3 3>&- 2>&4 4>&-

