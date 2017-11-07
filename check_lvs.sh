#!/bin/bash
# 功能说明： 切换读vip，检查读vip是否可用
# 脚本退出状态码说明： 0代表正常  1代表异常(触发切换读vip)
# 调用关系说明：调用ping_realserver.pl连接mysql检查是否健康

version='1.0.0'
# 高可用目录
keepDir=/etc/keepalived
# keepalived配置文件
keepConf=$keepDir/keepalived.conf
# 高可用配置文件
haConf=$keepDir/haconf
# 检查日志
checkLvsLog=/var/log/check_lvs.log

# 函数
print_log()
{
  _logfile=$1
  echo -e "${@:2}" >>$1
}

toupper()
{
  echo "$@"|/usr/bin/tr '[:lower:]' '[:upper:]'
}

# 检查命令行参数
arg=`echo $1|awk -F= '{print $1}'`
if [ "$arg" == "--version" ]; then 
  echo "`basename $0` $version";
  exit 0
elif [ "$arg" == "--help" ]; then
  echo "Usage: $0 {lvsVrrpInstance | --version | --help}"
  exit 0
fi

# 接收参数
lvsVrrpInstance=$1
[ ! -z $lvsVrrpInstance ] || { echo "lvsVrrpInstance is need"; exit 1; }
  
# 文件锁
lockfile=/tmp/check_lvs_${lvsVrrpInstance}.lock

(flock -n -e 2002 || { print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') WARN $0 is running, exit 0" ; exit 0 ; }

# 获取当前节点ip地址
localIp=`hostname -i`
  
# 获取所有数据库实例
dbPorts=$(grep -v '#' $haConf|awk '{IGNORECASE=1;if($8=="'$lvsVrrpInstance'") print $4}'|sort|uniq)
for dbPort in $dbPorts
do
  # 去掉换行符
  dbPort=$(echo $dbPort | tr -d '\r' | tr -d '\n')
  
  # 若haconf配置设置为"维护模式", 则正常退出，结束检查
  mode=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($8=="'$lvsVrrpInstance'") && ($4=="'$dbPort'")) print $1}'|uniq`
  mode=`toupper $mode`
  if [ $mode == "Y" ]; then
    print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO check haconf #mode is 'Y'"
    print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO $lvsVrrpInstance is in mantainance"
    exit 0
  fi
  
  # 若haconf配置设置为"强制切换"(即gotoFault=Y)，则切换读vip到另外keepalived节点
  gotoFault=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($8=="'$lvsVrrpInstance'") && ($4=="'$dbPort'")) print $7}'|uniq`
  gotoFault=`toupper $gotoFault`
  if [ $gotoFault == "Y" ]; then
    print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO check haconf #gotoFault is 'Y'"
    print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO $lvsVrrpInstance is in Fault"
    exit 1
  fi
  
  # 获取读vip
  readVip=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2=="lvs") && ($4=="'$dbPort'")) print $9}'|uniq`
  # 检查当前节点是否存在读vip，若当前节点上没有读vip，则不检查，直接exit 0
  /sbin/ip -4 -o a s|grep -w $readVip >/dev/null 2>&1
  # 若当前节点不存在读vip，则直接exit 0
  if [ $? -ne 0 ]; then
    isReadVip=0
    exit 0
  fi
  # 否则当前节点存在读vip
  isReadVip=1
  
  # 获取写vip
  writeVip=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2!="lvs") && ($4=="'$dbPort'")) print $9}'|uniq`
  # 判断当前节点是否存在写vip
  /sbin/ip -4 -o a s|grep -w $writeVip >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    # 当前节点存在写vip
    isWriteVip=1
  else
    # 当前节点不存在写vip
    isWriteVip=0   
  fi
  
  # 获取主库ip
  if [ $isWriteVip -eq 1 ]; then
    # 若当前节点存在写vip，则当前节点ip为主库ip
    masterIp=$localIp
  else
    # 当前节点不存在写vip，从haconf里面判断
    hagroupIp=$(grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2=="lvs") && ($4=="'$dbPort'")) print $6}')
    masterIp=$(echo $hagroupIp|awk 'BEGIN{FS="','"}{if ($1=="'$localIp'") print $2; else print $1}')
  fi
  print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO masterIp = $masterIp"
  
  #
  # 计算当前读vip下可用realserver数量(可用realserver条件: 权重大于0)
  availRealserverNum=$(/sbin/ipvsadm -Ln|egrep -w "Route|Local"|grep $dbPort|awk '{if($4>0) print $2}'|wc -l)
  print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO available realservers num: $availRealserverNum"
  if [ $availRealserverNum -eq 0 ];then
    # 若可用realserver等于0，则切换读vip
    exit 1
  elif [ $availRealserverNum -eq 1 ];then
    # 若可用realserver等于1, 则检查判断这个可用realserver是主库, 还是从库
    availRealserver=`/sbin/ipvsadm -Ln|egrep -w "Route|Local"|grep $dbPort|awk '{if ($4>0) print $2}'|awk -F: '{print $1}'`
    print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') INFO the only available realservers is $availRealserver"
    if [ $isWriteVip -eq 1 -a "$availRealserver" == "$masterIp" ]; then
      # 若可用realserver为主库,则连接读vip检查主库是否可用, 只需要检查mysql能否连接(--chk_dbconn_only=1)
      /etc/keepalived/ping_realserver.pl --host=$readVip --port=$dbPort --masterhost=$masterIp --chk_dbconn_only=1 >> $checkLvsLog 2>&1
      if [ $? -eq 0 ]; then
        # 主库正常, 则正常退出,不切换读vip
        exit 0
      else
        # 主库异常, 则异常退出,切换读vip
        exit 1
      fi
      # 否则可用realserver为从库,则连接读vip检查从库是否可用
      /etc/keepalived/ping_realserver.pl --host=$readVip --port=$dbPort --masterhost=$masterIp >> $checkLvsLog 2>&1
      if [ $? -eq 0 ]; then
        # 从库正常, 则正常退出,不切换读vip
        exit 0
      else
        # 从库异常，则切换读vip
        exit 1
      fi
    fi
  else
    # 可用realserver大于1, 则正常退出,不切换读vip
    exit 0
  fi
done
)2002>$lockfile

