#!/bin/bash
# 功能说明： 检查realserver健康
# 脚本退出状态码说明： 0代表正常 1代表异常
# 调用关系说明：调用ping_realserver.pl连接mysql检查从库是否健康

version='1.0.0'
# 高可用目录
keepDir=/etc/keepalived
# keepalived配置文件
keepConf=$keepDir/keepalived.conf
# 高可用配置文件
haConf=$keepDir/haconf
# 检查日志
realServerLog=/var/log/check_realserver.log

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
  echo "`basename $0` $version"
  exit 0
elif [ "$arg" == "--help" ]; then
  echo "Usage: $0 {realserverIp realserverPort | --version | --help}"
  exit 0
fi

# 接收参数
realserverIp=$1
realserverPort=$2
[ ! -z $realserverIp ] || { echo "realserverIp is need"; exit 0; }
[ ! -z $realserverPort ] || { echo "realserverPort is need"; exit 0; }

# 文件锁
lockfile=/tmp/check_realserver_${realserverIp}_${realserverPort}.lock  

(flock -n -e 2003 || { print_log $checkLvsLog "$(date '+%Y-%m-%d %H:%M:%S') WARN $0 is running, exit 0" ; exit 0 ; }  
  
  # 若haconf配置设置为“维护模式”，则正常退出，结束检查
  mode=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2=="lvs") && ($4=="'$realserverPort'")) print $1}'|uniq`
  mode=`toupper $mode`
  if [ $mode == "Y" ]; then
    print_log $realServerLog "$(date '+%Y-%m-%d %H:%M:%S') INFO check haconf #mode is 'Y'"
    print_log $realServerLog "$(date '+%Y-%m-%d %H:%M:%S') INFO $realserverPort is in mantainance"
    exit 0
  fi

  # 获取当前节点ip
  localIp=`hostname -i`
  
  # 获取读vip
  readVip=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2=="lvs") && ($4=="'$realserverPort'")) print $9}'|uniq`
  # 检查当前节点是否存在读vip，若当前节点上没有读vip，则不检查realserver，直接exit 0
  /sbin/ip -4 -o a s|grep -w $readVip >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    # 当前节点不存在读vip，直接exit 0
    isReadVip=0
    exit 0
  fi
  # 否则当前节点存在读vip
  isReadVip=1
  
  # 获取写vip
  writeVip=`grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2!="lvs") && ($4=="'$realserverPort'")) print $9}'|uniq`
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
    hagroupIp=$(grep -v "#" $haConf|awk '{IGNORECASE=1; if(($2=="lvs") && ($4=="'$realserverPort'")) print $6}')
    masterIp=$(echo $hagroupIp|awk 'BEGIN{FS="','"}{if ($1=="'$localIp'") print $2; else print $1}')
  fi
  
  # 若当前realserver为主库, 则需要根据实际情况是否让主库承担读服务 (不检查当前realserver健康状况)
  if [ $isReadVip -eq 1 -a $isWriteVip -eq 1 -a "$realserverIp" == "$masterIp" ]; then
  # 当前realserver为主库
    # 计算可用realserver数量 (可用realserver条件: 权重大于0)
    availRealserverNum=$(/sbin/ipvsadm -Ln|egrep -w "Route|Local"|grep $realserverPort|awk '{if($4>0) print $2}'|wc -l)
    if [ $availRealserverNum -gt 1 ]; then
      # 若可用realserver大于1,则主库也承担读服务
      exit 0
    elif [ $availRealserverNum -eq 1 ]; then
      # 可用realserver等于1
      # 检查权重
      dbconnect="${localIp}:${realserverPort}"		
      weight=$(/sbin/ipvsadm -Ln|egrep -w "Route|Local"|grep $realserverPort|awk '{if($2=="'$dbconnect'") {print $4}}')
      if [ $weight -eq 0 ]; then
        # 异常
        exit 1
      else
        # 正常
        exit 0
      fi  
    else
      # 若可用realserver等于0,则主库承担读服务
      exit 0
    fi
  fi
  # 否则当前realserver为从库, 直接检查realserver健康状况
  
  # 检查realserver健康状况
  /etc/keepalived/ping_realserver.pl --host=$realserverIp --port=$realserverPort --masterhost=$masterIp >> $realServerLog 2>&1
  if [ $? -eq 0 ]; then
    # realserver正常
    exit 0
  else
    # realserver异常
    exit 1
  fi
)2003>$lockfile

