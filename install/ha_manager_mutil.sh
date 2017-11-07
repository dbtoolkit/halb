#!/bin/bash
## hamgrl.sh
VERSION=1.1.3

print_call_stack_trace()
{
  logger_debug "call stack trace:"
  declare -i i
  for (( i=1; i<${#FUNCNAME[@]}; i++))
  {
    logger_debug " at ${FUNCNAME[$i]} (${BASH_SOURCE[$i]}:${BASH_LINENO[$((i-1))]})"
  }
}

### package os
MV=/bin/mv
AWK=awk
RM=/bin/rm
PS=/bin/ps
PGREP=/usr/bin/pgrep
PKILL=/usr/bin/pkill
KILL=kill
GREP=grep
SED=sed
TR=tr
CAT=cat
CUT=cut
getdirname() { declare -r n=${1:-$0}; dirname $n; }
getbasename() { declare -r n=${1:-$0}; echo ${n##*/}; }
getappname() { declare -r n=${1:-$0}; declare -r p=$(getbasename "$n"); echo ${p%%.*}; }
getappname2() { declare -r n=${1:-$0}; declare -r p=$(getbasename "$n"); echo ${p%.*}; }
ask()
{
  declare _read_var
  declare _msg=${1:-"Continue? "}
  declare _valid_var=${2:-"Y|N"}
  declare _good_var=$3
  declare _default_var=$4
  declare _case_insensitive=${5:-Y}
  if [ "$O_FLAG_INTERACTIVE" = "N" ]; then
    return 0
  fi
  _good_var=${_good_var:-$(echo $_valid_var|cut -d"|" -f1)}
  _default_var=${_default_var:-$_good_var}
  declare _s=$(echo "|$_valid_var|"|$SED "s/|$_default_var|/|[$_default_var]|/"|$SED 's/^|\+//;s/|\+$//')
  if [ "$_case_insensitive" != "N" ]; then
    _good_var=$(tolower "$_good_var")
    _default_var=$(tolower "$_default_var")
    _valid_var=$(tolower "$_valid_var")
  fi
  while : ; do
    echo -n "$_msg ($_s) "
    read _read_var
    if [ "$_case_insensitive" != "N" ]; then
      _read_var=$(tolower "$_read_var")
    fi
    _read_var=${_read_var:-$_default_var}
    if [ "$_read_var" = "$_good_var" ]; then
      return 0
    elif echo "|$_valid_var|" |$GREP -q -- "|$_read_var|" >/dev/null; then
      :
    else
      echo "bad answer."
    fi
  done
}
toupper()
{
  echo "$@"|$TR '[:lower:]' '[:upper:]'
}
tolower()
{
  echo "$@"|$TR '[:upper:]' '[:lower:]'
}
os_get_hostip()
{
  getent hosts $(hostname)|head -n1|awk '{print $1}'
}
os_get_interface()
{
  /sbin/ip -4 -o link show up|$GREP -v "loopback"|$AWK '{print $2}'|$GREP -v usb|head -n1|cut -d: -f1
}
in_array() {
    local hay needle=$1
    shift
    for hay; do
        [[ $hay == $needle ]] && return 0
    done
    return 1
}
declare -a _hash_list=()
hash_init() { rm -rf $TMPDIR/hashmap.$1; mkdir -p $TMPDIR/hashmap.$1/; declare -i _i=${#_hash_list[@]}; $_hash_list[$_i]=$1; }
hash_put() { printf "$3" > $TMPDIR/hashmap.$1/$2; }
hash_get() { cat $TMPDIR/hashmap.$1/$2; }
hash_rm() { rm -rf $TMPDIR/hashmap.$1/$2; }
hash_rmall() { rm -rf $TMPDIR/hashmap.$1/*; }
hash_keys() { ls -1 $TMPDIR/hashmap.$1/; }
hash_drop() { rm -rf $TMPDIR/hashmap.$1; }
hash_dropall() { rm -rf $TMPDIR/hashmap.*; }

SSH="ssh -o ConnectTimeout=10"
check_vip()
{
  declare -r _host=$1
  declare -r _vip=$2
  declare _rc=0
  if $SSH root@$_host "/sbin/ip -4 -o a s" >$IP_TMP; then
    if grep -q " ${_vip}/" $IP_TMP; then
      _rc=0
    else
      _rc=1
    fi
  else
    _rc=2
  fi
  return $_rc
}
check_vip_local()
{
  declare -r _vip=$1
  declare _rc=0
  if /sbin/ip -4 -o a s >$IP_TMP; then
    if grep -q " ${_vip}/" $IP_TMP; then
      _rc=0
    else
      _rc=1
    fi
  else
    _rc=2
  fi
  return $_rc
}
check_network_connectivity()
{
  declare -r _host=$1
  declare -r _check=${2:-ssh}
  case "$_check" in
    "ping")
      if ! ping -c 5 -i 1 -q -W 5 $_host|grep -q " 0% packet loss"; then
        return 1
      fi
      ;;
    "ssh")
      $SSH root@$_host ":" || return 2
      ;;
    *)
      return 3
      ;;
  esac
}
check_ssh_connectivity()
{
  check_network_connectivity "$1" "ssh"
}
check_ping_connectivity()
{
  check_network_connectivity "$1" "ping"
}


### package mysql
MYTAB=/etc/mytab
MONGOTAB=/etc/mongotab
mysql_get_param_by_port()
{
  local _param_name=$1
  local _port=$2
  local _param_value=$(\ps -e -o command|grep "mysqld "|grep -- "--port=$_port"|grep -v grep|head -n1|sed 's/ --/\n--/g'|sed -n '/^--'$_param_name'=/s/^--'$_param_name'=//p'|head -n1)
  echo $_param_value
}
mysql_get_param_by_conf()
{
  local _param_name=$1
  local _mysqld=$2
  local _conffile=$3
  local _param_value=$($_mysqld --defaults-file=${_conffile} --print-defaults|sed '2,$s/ --/\n--/g'|sed -n '/^--'$_param_name'=/s/^--'$_param_name'=//p')
  echo $_param_value
}
mysql_get_param()
{
  local _mysqld=$1
  local _conffile=$2
  local _port=$3
  local _param_name=$4
  local _param_value=$(mysql_get_param_by_conf "$_param_name" "$_mysqld" "$_conffile")
  [ -z "$_param_value" ] && _param_value=$(mysql_get_param_by_port "$_param_name" "$_port")
  echo $_param_value
}
mysql_get_mysqld()
{
  local _mysql_home=$1
  local _mysqld=$_mysql_home/bin/mysqld
  [ ! -x $_mysqld ] && _mysqld=$_mysql_home/libexec/mysqld
  echo $_mysqld
}
mysql_get_mysqld_by_port()
{
  local _port=$1
  local _mysqld=$(/bin/ps -eo command|grep -- "/mysqld .*--port=$_port[ ]*"|grep -v grep|head -n1|awk '{print $1}')
  echo $_mysqld
}
mysql_get_conf()
{
  local _port=$1
  echo $(mysql_get_param_by_port defaults-file $_port)
}
mysql_get_logon_param()
{
  declare -r _mysql_home=$1
  declare -r _conffile=$2
  declare -r _port=$3
  declare -r _mysqld=$(mysql_get_mysqld $_mysql_home)
  declare -r _socket=$(mysql_get_param "$_mysqld" "$_conffile" "$_port" socket)
  declare -r _mysql=$_mysql_home/bin/mysql
  declare _logonparams=""
  _logonparams="-h127.0.0.1 -P$_port -uroot"
  $_mysql $_logonparams -e"select user()" --batch --skip-column-names >/dev/null 2>&1 && { echo "$_logonparams"; return; }
  _logonparams="-S$_socket -uroot"
  $_mysql $_logonparams -e"select user()" --batch --skip-column-names >/dev/null 2>&1 && { echo "$_logonparams"; return; }
  #_logonparams="-h$(os_get_hostip) -P$_port -umysqlmon -p"'xxxxxxxx'""
  #$_mysql $_logonparams -e"select user()" --batch --skip-column-names >/dev/null 2>&1 && { echo "$_logonparams"; return; }
}
mysql_get_mysqld_by_port()
{
  local _port=$1
  local _mysqld=$(/bin/ps -eo command|grep -- "/mysqld .*--port=$_port[ ]*"|grep -v grep|head -n1|awk '{print $1}')
  echo $_mysqld
}
mysql_get_dbhome_by_port()
{
  declare -r _port=$1
  declare _home
  if [ -f $MYTAB ]; then
    _home=$(sed '/^#/d;/^[[:space:]]*$/d' $MYTAB |awk '{if ($3=="'$_port'") print $1}')
  fi
  if [ "$_home" = "" ]; then
    declare _mysqld=$(mysql_get_mysqld_by_port $_port)
    if [ "$_mysqld" != "" ]; then
      _home=${_mysqld%/*/*}
    fi
  fi
  echo $_home
}

mongodb_get_mongos_by_port()
{
  local _port=$1
  local _mongos=$(/bin/ps -eo command|grep -- "/mongos .*$_port[ ]*"|grep -v grep|head -n1|awk '{print $1}')
  echo $_mongos
}
mongodb_get_dbhome_by_port()
{
  declare -r _port=$1
  declare _home
  if [ -f $MONGOTAB ]; then
    _home=$(sed '/^#/d;/^[[:space:]]*$/d' $MONGOTAB |awk '{if ($3=="'$_port'") print $1}')
  fi
  if [ "$_home" = "" ]; then
    declare _mysqld=$(mongodb_get_mongos_by_port $_port)
    if [ "$_mongos" != "" ]; then
      _home=${_mongos%/*/*}
    fi
  fi
  echo $_home
}

mysql_get_conf_by_port()
{
  declare -r _port=$1
  declare _conf
  if [ -f $MYTAB ]; then
    _conf=$(sed '/^#/d;/^[[:space:]]*$/d' $MYTAB |awk '{if ($3=="'$_port'") print $2}')
  fi
  if [ "$_conf" = "" ]; then
    _conf=$(mysql_get_conf $_port)
  fi
  echo $_conf
}

mysql_get_slave_ip_by_port()
{
  declare -r _mysql_home=$1
  declare -r _port=$2
  declare -r _mysql_conf=$(mysql_get_conf_by_port $_port)
  declare -r _logon_param=$(mysql_get_logon_param "$_mysql_home" "$_mysql_conf" "$_port")
  declare -r _mysql="$_mysql_home/bin/mysql $_logon_param"
  declare _slave_ip=$($_mysql --batch --silent --delimiter=$'\t' --skip-column-names -e "show processlist"|grep "Binlog Dump"|awk '{print $3}'|cut -d: -f1)
  echo $_slave_ip
}

###package oracle
ORATAB=/etc/oratab
oracle_get_oracle_by_sid()
{
  declare _sid=$1
  declare _pmon_pid=$(/bin/ps -eo pid,command|awk '{if ($2=="ora_pmon_'$_sid'") print $1}'|head -n1)
  declare _exe=$(readlink /proc/$_pmon_pid/exe)
  echo $_exe
}
oracle_get_dbhome_by_sid()
{
  declare -r _sid=$1
  declare _home
  if [ -f $ORATAB ]; then
    _home=$(sed '/^#/d;/^[[:space:]]*$/d' $ORATAB |awk -F: '{if ($1=="'$_sid'") print $2}')
  fi
  if [ "$_home" = "" ]; then
    declare _oracle=$(oracle_get_oracle_by_sid $_sid)
    if [ "$_oracle" != "" ]; then
      _home=${_oracle%/*/*}
    fi
  fi
  echo $_home
}
oracle_get_standby_ip_by_sid()
{
  declare -r _sid=$1
  declare _profile=~oracle/.${_sid}profile
  declare _standby_dbs=$(su - oracle -c '. '$_profile';dgmgrl -silent / "show configuration"'|grep "Physical standby database"|awk '{print $1}')
  declare _standby_ip
  declare _d
  for _d in $_standby_dbs; do
    _standby_ip="$_standby_ip $(su - oracle -c '. '$_profile';dgmgrl -silent / "show database verbose '$_d'"'|egrep "DGConnectIdentifier|InitialConnectIdentifier"|awk '{print $3}'|sed -n 's/.*(HOST=\([^\)]*\)).*/\1/p')"
  done
  echo $_standby_ip
}

### package keepalived
# haconf
##maintainance	dbtype	dbhome	sidorport	dggroup	dest_ip	gotofault	vrrpinstance
#N	Mysql	/usr/local/mysql-5.1.50-linux-x86_64-glibc23	3307	null	192.168.8.21,192.168.8.26	N	panda
#N	MySQL	/usr/local/mysql-5.1.50-linux-x86_64-glibc23	3308	null	192.168.8.21,192.168.8.26	N	oogway
#N	Proxy	/usr/local/mysql-5.1.50-linux-x86_64-glibc23	4040	null	192.168.8.21,192.168.8.26   N	panda
KEEPALIVED_ETC=/etc/keepalived
HACONF=$KEEPALIVED_ETC/haconf
KEEPALIVED_CONF=$KEEPALIVED_ETC/keepalived.conf
KEEPALIVED_INITD=/etc/init.d/keepalived

##
# helper
##
ka_show_block_in_config()
{
  declare -r _block=$1
  $AWK '
/^[[:space:]]*'"$_block"'[[:space:]]+{[[:space:]]*$/ {
  print
  I=1
  while (I>0) {
    getline
    if ($0 ~ /}/) I--
    if ($0 ~ /{/) I++
    print
  }
}
'
}
ka_get_instance_vip_from_keepalived_conf()
{
  declare -r _inst=$1
  $SED '/^#/d;/^[[:space:]]*$/d' $KEEPALIVED_CONF |\
  ka_show_block_in_config "vrrp_instance[[:space:]]+$_inst" |\
  ka_show_block_in_config "virtual_ipaddress" |\
  $SED -n '2{p;q}'| $AWK '{print $1}'
}
ka_check_file()
{
  if [[ $O_TARGET == "ha" || $O_TARGET == "all" ]]; then
    if [ ! -f "$HACONF" ] ; then
      logger_warn "$HACONF not found, auto-create haconf file."
      touch $HACONF
    fi
  fi
  if [[ $O_TARGET == "keepalived" || $O_TARGET == "all" ]]; then
    if [ ! -f "$KEEPALIVED_CONF" ] ; then
      logger_warn "$KEEPALIVED_CONF not found, auto-create keepalived.conf file."
      touch $KEEPALIVED_CONF
    fi
  fi
}
file_overwrite()
{
  declare -r _new=$1
  declare -r _old=$2
  if [ "$O_FLAG_DEBUG" = "Y" ]; then
    #cat $_new
    if [ -f "$_new" -a ! -f "$_old" ]; then
      cat "$_new"
    elif [ ! -f "$_new" -a -f "$_old" ]; then
      cat "$_old"
    else
      diff $_new $_old
    fi
  fi
  if [ "$O_DRY_RUN" != "Y" ]; then
    mv $_new $_old
  fi
}
get_virtual_router_id()
{
  declare _vip=$1
  echo "$_vip"|$AWK -F. '{if ($3=="96") print "255"; else print $4}'
}
get_virtual_router_id_in_vrrpinstance()
{
  declare _vi=$1
  echo "$_vi"|$AWK -F'_' '{print $3}'
}
get_dbtype_in_vrrpinstance()
{
  declare _vi=$1
  echo "$_vi"|$AWK -F'_' '{print $2}'
}
ka_conf_formatter()
{
  $SED -e 's/#.*//;s/{/\n{\n/g;s/}/\n}\n/g' | \
  $SED -e 's/^[[:space:]]\+//;s/[[:space:]]\+$//;/^[[:space:]]*$/d;s/[[:space:]]\+/ /g' | \
  $SED -e :a -e '$!N;s/\n{/ {/;ta' -e 'P;D' | \
  $AWK 'function Ind(lvl,    i) {for(i=1;i<=lvl;i++) printf "%s", IND}
BEGIN {IND="    ";level=0;nextlevel=0}
/ {$/ {nextlevel++}
/^}$/ {nextlevel--;level--}
{Ind(level);print;level=nextlevel}'
}


##
# backup/restore conf
##
backup_conf_file()
{
  declare _source=${1}
  declare _backup=${2}
  declare -i _r=0
  if [ -f "$_source" ]; then
    if [ -f "$_backup" ]; then
      logger_warn "backup file $_backup will be overwritten"
    fi
    logger_debug "backuping $_source to $_backup ..."
    if [ "$O_DRY_RUN" != "Y" ]; then
      \cp -fp "$_source" "$_backup" || _r=2
    fi
  else
    logger_warn "source file $_source not found"
    _r=1
  fi
  return $_r
}
restore_conf_file()
{
  declare _target=${1}
  declare _backup=${2}
  declare _preserve_backup=${3:-Y}
  declare -i _r=0
  if [ -f "$_backup" ]; then
    if [ -f "$_target" ]; then
      logger_warn "target file $_target will be overwritten"
    fi
    logger_debug "restoring $_backup to $_target ..."
    if [ "$_preserve_backup" = "Y" ]; then
      logger_debug "cp $_backup to $_target ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        \cp -fp "$_backup" "$_target" || _r=2
      fi
    else
      logger_debug "mv $_backup to $_target ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        \mv -f "$_backup" "$_target" || _r=3
      fi
    fi
  else
    logger_warn "backup file $_backup not found"
    _r=1
  fi
  return $_r
}
remove_conf_file()
{
  declare _target=${1}
  declare -i _r=0
  if [ -f "$_target" ]; then
    logger_debug "rm $_target ..."
    if [ "$O_DRY_RUN" != "Y" ]; then
      \rm -f "$_target" || _r=1
    fi
  fi
  return $_r
}
backup_conf()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _o_backup_conf=${1:-$O_BACKUP_CONF}
  declare _backup_ts=${2:-$O_BACKUP_TS}
  declare _haconf=$HACONF
  declare _keepalived_conf=$KEEPALIVED_CONF
  declare _haconf_bak=${HACONF_BAK:-"${_haconf}.bak.${_backup_ts}"}
  declare _keepalived_conf_bak=${KEEPALIVED_CONF_BAK:-"${_keepalived_conf}.bak.${_backup_ts}"}
  declare -i _r=0
  declare -i _ec_haconf=1
  declare -i _ec_keepalived_conf=2
  declare _b=${_o_backup_conf:-all}
  #for _b in ${_o_backup_conf//,/ }; do
    if [ "$_b" = "all" -o "$_b" = "ha" ]; then
      logger_debug "_haconf_bak=$_haconf_bak"
      logger_info "backuping haconf ..."  
      backup_conf_file "$_haconf" "$_haconf_bak" || ((_r+=_ec_haconf))
    fi
    if [ "$_b" = "all" -o "$_b" = "keepalived" ]; then
      logger_debug "_keepalived_conf_bak=$_keepalived_conf_bak"
      logger_info "backuping keepalived.conf ..."  
      backup_conf_file "$_keepalived_conf" "$_keepalived_conf_bak" || ((_r+=_ec_keepalived_conf))
    fi
  #done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}
restore_conf()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare _o_restore_conf=${1:-$O_RESTORE_CONF}
  declare _restore_ts=${2:-$O_RESTORE_TS}
  declare _o_backup_conf=${3:-$O_BACKUP_CONF}
  declare _backup_ts=${4:-$O_BACKUP_TS}
  declare -r _restore_preserve_backup=${5:-$O_RESTORE_PRESERVE_BACKUP}
  declare _haconf=$HACONF
  declare _keepalived_conf=$KEEPALIVED_CONF
  declare _haconf_bak=$HACONF_BAK
  declare _keepalived_conf_bak=$KEEPALIVED_CONF_BAK
  _o_restore_conf=${_o_restore_conf:-all}
  _o_backup_conf=${_o_backup_conf:-${_o_restore_conf}}

  declare -i _r=0
  _r=1
  if [ "$_o_restore_conf" = "all" -o "$_o_restore_conf" = "all" ]; then
    if [ -z "$_haconf_bak" ]; then
      if [ -z "$_restore_ts" ]; then
        _haconf_bak=$(\ls -1 ${_haconf}.bak.[0-9][0-9]* | tail -n1)
      else
        _haconf_bak=${_haconf}.bak.${_restore_ts}
      fi
    fi
    if [ -z "$_haconf_bak" -o ! -f "$_haconf_bak" ]; then
      logger_error "haconf backup not found"
    else
      logger_debug "_haconf_bak=$_haconf_bak"
      if [ "$_o_backup_conf" = "all" -o "$_o_backup_conf" = "ha" ]; then
        logger_info "backuping haconf before restoring ..."  
        backup_conf "ha" "$_backup_ts"
      fi
      logger_info "restoring haconf ..."  
      restore_conf_file "$_haconf" "$_haconf_bak" "$_restore_preserve_backup" && _r=0
    fi
  fi
  _r=2
  if [ "$_o_restore_conf" = "all" -o "$_o_restore_conf" = "keepalived" ]; then
    if [ -z "$_keepalived_conf_bak" ]; then
      if [ -z "$_restore_ts" ]; then
        _keepalived_conf_bak=$(\ls -1 ${_keepalived_conf}.bak.[0-9][0-9]* | tail -n1)
      else
        _keepalived_conf_bak=${_keepalived_conf}.bak.${_restore_ts}
      fi
    fi
    if [ -z "$_keepalived_conf_bak" ]; then
      logger_error "keepalived.conf backup not found"
    else
      logger_debug "_keepalived_conf_bak=$_keepalived_conf_bak"
      if [ "$_o_backup_conf" = "all" -o "$_o_backup_conf" = "keepalived" ]; then
        logger_info "backuping keepalived.conf before restoring ..."  
        backup_conf "keepalived" "$_backup_ts"
      fi
      logger_info "restoring keepalived.conf ..."  
      restore_conf_file "$_keepalived_conf" "$_keepalived_conf_bak" "$_restore_preserve_backup" && _r=0
    fi
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}
remove_conf()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _o_backup_conf=${1:-$O_BACKUP_CONF}
  declare _backup_ts=${2:-$O_BACKUP_TS}
  declare _haconf=$HACONF
  declare _keepalived_conf=$KEEPALIVED_CONF
  declare _haconf_bak=${HACONF_BAK:-"${_haconf}.bak.${_backup_ts}"}
  declare _keepalived_conf_bak=${KEEPALIVED_CONF_BAK:-"${_keepalived_conf}.bak.${_backup_ts}"}
  declare -i _r=0
  declare -i _ec_haconf=1
  declare -i _ec_keepalived_conf=2
  if [ "$_o_backup_conf" = "all" -o "$_o_backup_conf" = "ha" ]; then
    logger_debug "_haconf_bak=$_haconf_bak"
    logger_info "removing haconf ..."  
    remove_conf_file "$_haconf" "$_haconf_bak" || ((_r+=_ec_haconf))
  fi
  if [ "$_o_backup_conf" = "all" -o "$_o_backup_conf" = "keepalived" ]; then
    logger_debug "_keepalived_conf_bak=$_keepalived_conf_bak"
    logger_info "removing keepalived.conf ..."  
    remove_conf_file "$_keepalived_conf" "$_keepalived_conf_bak" || ((_r+=_ec_keepalived_conf))
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}


##
# set variables
##
eval_param()
{
  declare -r _k=${1%%=*}
  declare -r _v=${1#*=}
  declare -r _f="O_P_$(toupper "$_k")=${_v}"
  logger_debug "eval $_f"
  eval $_f
  #if [[ $_k == "dest_ip" || $_k == "realservergrp" ]]; then
  #  declare _e
  #  declare -i _i=0
  #  for _e in ${_v//,/ }; do
  #    declare _ff="O_P_$(toupper "$_k")_ARRAY[$_i]=${_e}"
  #    logger_debug "eval $_ff"
  #    eval $_ff
  #    ((_i++))
  #  done
  #fi
}
set_dest_ip_array()
{
  declare -i _i
  declare _s
  unset O_P_DEST_IP_ARRAY
  _i=1
  for _s in ${O_P_DEST_IP//,/ }; do
    # destination ip same with physical ip is in the first position
    if [[ $_s == $PHYSICAL_IP ]]; then
      O_P_DEST_IP_ARRAY[0]=$_s
    else
      O_P_DEST_IP_ARRAY[$_i]=$_s
      ((_i++))
    fi
  done
}
set_realservergrp_array()
{
  declare -i _i
  declare _s
  unset O_P_REALSERVERGRP_ARRAY
  _i=0
  for _s in ${O_P_REALSERVERGRP//,/ }; do
    O_P_REALSERVERGRP_ARRAY[$_i]=$_s
    ((_i++))
  done
}
set_realserver_array()
{
  declare -i _i
  declare _s
  unset O_REALSERVER_ARRAY
  _i=0
  for _s in ${O_REALSERVER//,/ }; do
    O_REALSERVER_ARRAY[$_i]=$_s
    ((_i++))
  done
}
set_sorryserver_array()
{
  declare -i _i
  declare _s
  unset O_SORRYSERVER_ARRAY
  _i=0
  for _s in ${O_SORRYSERVER//,/ }; do
    O_SORRYSERVER_ARRAY[$_i]=$_s
    ((_i++))
  done
}
read_variables_in_haconf()
{
  declare -r _vi=$O_VRRPINSTANCE
  declare -r _vip=$O_VIP
  declare -r _dbtype=$O_DBTYPE
  declare -r _sidorport=$O_SIDORPORT
  logger_info "++++++++++++++++++++++++port++++++++++++++++++++++${_sidorport}"
  declare _line
  _line=$($AWK "{if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\") ) print}" $HACONF)
  logger_debug "_line=$_line"
  O_P_MAINTAINANCE=$(echo "$_line" | $AWK '{print $1}')
  O_P_DBTYPE=$(echo "$_line" | $AWK '{print $2}')
  O_P_DBHOME=$(echo "$_line" | $AWK '{print $3}')
  O_P_SIDORPORT=$(echo "$_line" | $AWK '{print $4}')
  O_P_DGGROUP=$(echo "$_line" | $AWK '{print $5}')
  O_P_DEST_IP=$(echo "$_line" | $AWK '{print $6}')
  O_P_GOTOFAULT=$(echo "$_line" | $AWK '{print $7}')
  O_P_VRRPINSTANCE=$(echo "$_line" | $AWK '{print $8}')
  O_P_VIP=$(echo "$_line" | $AWK '{print $9}')
  O_P_REALSERVERGRP=$(echo "$_line" | $AWK '{print $10}')
  O_P_VIRTUAL_ROUTER_ID=$(echo "$O_P_VRRPINSTANCE" | $AWK -F'_' '{print $3}')
  #O_P_VRID="$O_P_VIRTUAL_ROUTER_ID"
}


##
# clear variables
##
clear_variables()
{
  unset O_P_MAINTAINANCE
  unset O_P_DBTYPE
  unset O_P_DBHOME
  unset O_P_SIDORPORT
  unset O_P_DGGROUP
  unset O_P_DEST_IP
  unset O_P_GOTOFAULT
  unset O_P_VRRPINSTANCE
  unset O_P_VIP
  unset O_P_REALSERVERGRP
  unset O_P_DEST_IP_ARRAY
  unset O_P_REALSERVERGRP_ARRAY
  unset O_P_VIRTUAL_ROUTER_ID
  unset O_P_VI_INTERFACE
  unset O_P_VSRV_WEIGHT
  unset O_P_VRID
}
print_variables()
{
  declare -r _lvl=${1:-DEBUG}
  declare _f=logger_debug
  case $_lvl in
  "DEBUG") _f=logger_debug ;;
  "INFO") _f=logger_info ;;
  esac
  $_f "==ha variables=="
  $_f "MAINTAINANCE  : $O_P_MAINTAINANCE"
  $_f "DBTYPE        : $O_P_DBTYPE"
  $_f "DBHOME        : $O_P_DBHOME"
  $_f "SIDORPORT     : $O_P_SIDORPORT"
  $_f "DGGROUP       : $O_P_DGGROUP"
  $_f "DEST_IP       : $O_P_DEST_IP"
  $_f "GOTOFAULT     : $O_P_GOTOFAULT"
  $_f "VRRPINSTANCE  : $O_P_VRRPINSTANCE"
  $_f "VIP           : $O_P_VIP"
  $_f "REALSERVERGRP : $O_P_REALSERVERGRP"
  declare -i _i=0
  for ((_i=0; _i<${#O_P_DEST_IP_ARRAY[@]}; _i++)); do
    $_f "O_P_DEST_IP_ARRAY[$_i]: ${O_P_DEST_IP_ARRAY[$_i]}"
  done
  for ((_i=0; _i<${#O_P_REALSERVERGRP_ARRAY[@]}; _i++)); do
    $_f "O_P_REALSERVERGRP_ARRAY[$_i]: ${O_P_REALSERVERGRP_ARRAY[$_i]}"
  done
  $_f "O_P_VIRTUAL_ROUTER_ID: $O_P_VIRTUAL_ROUTER_ID"
  $_f "O_P_VI_INTERFACE     : $O_P_VI_INTERFACE"
  $_f "O_P_VSRV_WEIGHT      : $O_P_VSRV_WEIGHT"
  $_f "O_P_VRID             : $O_P_VRID"
  $_f "==end=="
}


##
# check variables
##
set_default_variables()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1

  O_P_DBTYPE=$(tolower "$O_P_DBTYPE")
  O_P_VRRPINSTANCE=$(tolower "$O_P_VRRPINSTANCE")
  O_P_MAINTAINANCE=$(toupper "$O_P_MAINTAINANCE")
  O_P_GOTOFAULT=$(toupper "$O_P_GOTOFAULT")

  if [[ -z $O_P_VI_INTERFACE ]]; then
    logger_debug "O_P_VI_INTERFACE is set to default"
    O_P_VI_INTERFACE=$PHYSICAL_INTERFACE
  fi

  if [[ -z $O_P_DBTYPE ]]; then
    logger_debug "O_P_DBTYPE is set to default"
    O_P_DBTYPE=$_dbtype
  fi

  if [[ -z $O_P_VIRTUAL_ROUTER_ID ]]; then
    declare -r _vrid=$(get_virtual_router_id "$O_P_VIP")
    logger_debug "O_P_VIRTUAL_ROUTER_ID is set to default"
    O_P_VIRTUAL_ROUTER_ID=$_vrid
  fi

  if [[ -z "$O_P_VRID" ]]; then
    logger_debug "O_P_VRID is set to default"
    O_P_VRID=$O_P_VIRTUAL_ROUTER_ID
  fi

  if [[ -z $O_P_VRRPINSTANCE ]]; then
    logger_debug "O_P_VRRPINSTANCE is set to default"
    O_P_VRRPINSTANCE="vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"
  fi

  if [[ $(tolower "$O_P_DGGROUP") == "null" ]]; then
    logger_debug "O_P_DGGROUP is set to null"
    O_P_DGGROUP=""
  fi

  if [[ -z $O_P_SIDORPORT ]]; then
    logger_debug "O_P_SIDORPORT is set to O_SIDORPORT"
    O_P_SIDORPORT=$O_SIDORPORT
  fi
  if [[ -z $O_P_SIDORPORT ]]; then
    logger_debug "O_P_SIDORPORT is set to default"
    O_P_SIDORPORT="3306"
  fi

  if [[ $_dbtype == "mysql" || $_dbtype == "lvs" || $_dbtype == "pxcw" || $_dbtype == "pxcr" ]]; then
    if [[ -z $O_P_DBHOME ]]; then
      logger_debug "get O_P_DBHOME from mytab"
      O_P_DBHOME=$(mysql_get_dbhome_by_port "$O_P_SIDORPORT")
    fi
    if [[ -z $O_P_DBHOME ]]; then
      logger_debug "O_P_DBHOME is set to default"
      O_P_DBHOME="/usr"
    fi
  elif [[ $_dbtype == "mongodb" ]]; then
     if [[ -z $O_P_DBHOME ]]; then
      logger_debug "get O_P_DBHOME from mongotab"
      O_P_DBHOME=$(mongodb_get_dbhome_by_port "$O_P_SIDORPORT")
    fi
    if [[ -z $O_P_DBHOME ]]; then
      logger_debug "O_P_DBHOME is set to default"
      O_P_DBHOME="/usr"
    fi
  elif [[ $_dbtype == "oracle" ]]; then
    logger_error "oracle dbhome is not applicable for now."
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ -z $O_P_MAINTAINANCE ]]; then
    logger_debug "O_P_MAINTAINANCE is set to default"
    O_P_MAINTAINANCE="N"
  fi
  if [[ -z $O_P_GOTOFAULT ]]; then
    logger_debug "O_P_GOTOFAULT is set to default"
    O_P_GOTOFAULT="N"
  fi

  if [[ $(tolower "$O_P_REALSERVERGRP") == "null" ]]; then
    logger_debug "O_P_REALSERVERGRP is set to null"
    O_P_REALSERVERGRP=""
  fi
  set_realservergrp_array

  if [[ -z $O_P_VSRV_WEIGHT ]]; then
    logger_debug "O_P_VSRV_WEIGHT is set to default"
    O_P_VSRV_WEIGHT=5
  fi

  if [[ -z $O_P_DEST_IP ]]; then
    logger_debug "O_P_DEST_IP is set to default"
    O_P_DEST_IP=$PHYSICAL_IP
  fi
  set_dest_ip_array
  declare -i _i
  declare _s
  for ((_i=0; _i<${#O_P_DEST_IP_ARRAY[@]}; _i++)); do
    _s="${_s},${O_P_DEST_IP_ARRAY[_i]}"
  done
  _s=${_s/#,/}
  if [[ $_s != $O_P_DEST_IP ]]; then
    logger_debug "resetting O_P_DEST_IP ..."
    O_P_DEST_IP=$_s
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
check_variables()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1

  logger_debug "checking physical ip ..."
  if [[ -z $PHYSICAL_IP ]]; then
    logger_error "PHYSICAL_IP is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking physical interface ..."
  if [[ -z $PHYSICAL_INTERFACE ]]; then
    logger_error "PHYSICAL_INTERFACE is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable vi interface ..."
  if [[ -z $O_P_VI_INTERFACE ]]; then
    logger_error "O_P_VI_INTERFACE is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $O_P_VI_INTERFACE != $PHYSICAL_INTERFACE ]]; then
    logger_error "O_P_VI_INTERFACE maybe invalid"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable maintainance ..."
  if [[ -z $O_P_MAINTAINANCE ]]; then
    logger_error "O_P_MAINTAINANCE is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $O_P_MAINTAINANCE != "N" && $O_P_MAINTAINANCE != "Y" ]]; then
    logger_debug "O_P_MAINTAINANCE is invalid: $O_P_MAINTAINANCE"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable dbtype ..."
  if [[ -z $O_P_DBTYPE ]]; then
    logger_error "O_P_DBTYPE is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $O_P_DBTYPE != "mysql" && $O_P_DBTYPE != "lvs" && $O_P_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "O_P_DBTYPE is invalid: $O_P_DBTYPE"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $O_P_DBTYPE != $_dbtype ]]; then
    logger_error "O_P_DBTYPE is invalid"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable dbhome ..."
  if [[ -z $O_P_DBHOME ]]; then
    logger_error "O_P_DBHOME is null"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $_dbtype == "mysql" || $_dbtype == "lvs" || $_dbtype == "pxcw" || $_dbtype == "pxcr" ]]; then
    if [[ ! -x $O_P_DBHOME/bin/mysql ]]; then
      logger_error "mysql is not installed in dbhome: $O_P_DBHOME"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  elif [[ $_dbtype == "mongodb" ]]; then
    if [[ ! -x $O_P_DBHOME/bin/mongos ]]; then
      logger_error "mongodb is not installed in dbhome: $O_P_DBHOME"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  elif [[ $_dbtype == "oracle" ]]; then
    if [[ ! -x $O_P_DBHOME/bin/sqlplus ]]; then
      logger_error "sqlplus is not installed in dbhome: $O_P_DBHOME"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
    if [[ ! -x $O_P_DBHOME/bin/dgmgrl ]]; then
      logger_error "dgmgrl is not installed in dbhome: $O_P_DBHOME"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  fi

  logger_debug "checking variable sidorport ..."
  if [[ -z $O_P_SIDORPORT ]]; then
    logger_error "O_P_SIDORPORT is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable dbgroup ..."
  if [[ $_dbtype == "mysql" || $_dbtype == "lvs" ]]; then
    if [[ -n $O_P_DGGROUP && $O_P_DGGROUP != "null" ]]; then
      logger_error "O_P_DGGROUP is not null"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  elif [[ $_dbtype == "oracle" ]]; then
    if [[ -z $O_P_DGGROUP || $O_P_DGGROUP == "null" ]]; then
      logger_error "O_P_DGGROUP is null"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  fi

  logger_debug "checking variable dest ip ..."
  if [[ -z $O_P_DEST_IP ]]; then
    logger_error "O_P_DEST_IP is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ ${#O_P_DEST_IP_ARRAY[@]} -lt 1 ]]; then
    logger_error "number of dest ip is less than 1: $O_P_DEST_IP"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ ${#O_P_DEST_IP_ARRAY[@]} -gt 2 ]]; then
    logger_error "number of dest ip is bigger than 2: $O_P_DEST_IP"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ -z ${O_P_DEST_IP_ARRAY[0]} ]]; then
    logger_error "O_P_DEST_IP_ARRAY[0] is null , local physical ip not exist dest_ip string ?"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ ${O_P_DEST_IP_ARRAY[0]} != $PHYSICAL_IP ]]; then
    logger_error "O_P_DEST_IP_ARRAY[0] differ from PHYSICAL_IP"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable gotofault ..."
  if [[ -z $O_P_GOTOFAULT ]]; then
    logger_error "O_P_GOTOFAULT is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  if [[ $O_P_GOTOFAULT != "N" && $O_P_GOTOFAULT != "Y" ]]; then
    logger_debug "O_P_GOTOFAULT is invalid: $O_P_GOTOFAULT"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable vip ..."
  if [[ -z $O_P_VIP ]]; then
    logger_error "O_P_VIP is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  logger_debug "checking variable virtual router id ..."
  if [[ -z $O_P_VIRTUAL_ROUTER_ID ]]; then
    logger_error "O_P_VIRTUAL_ROUTER_ID is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  declare -r _vrid=$(get_virtual_router_id "$O_P_VIP")
#echo "----->vrid---->$O_P_VIP"
#echo "----->O_P_VIRTUAL_ROUTER_ID---->$O_P_VIRTUAL_ROUTER_ID"
#echo "----->vrid---->$vrid"
  if [[ $_dbtype != "pxcr" && $_dbtype != "mongodb"  ]];then
    if [[ $O_P_VIRTUAL_ROUTER_ID != $_vrid ]]; then
      logger_error "O_P_VIRTUAL_ROUTER_ID is invalid"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  fi

  if [[ -z $O_P_VRID ]]; then
    logger_error "O_P_VRID is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  declare -ir _vrid_tmp=$O_P_VRID
  if ! (( _vrid_tmp >= 1 && _vrid_tmp <=255 )); then
    logger_error "O_P_VRID is out of range(1-255): $O_P_VRID"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi

  #previous logic
  #if [[ $O_P_VIRTUAL_ROUTER_ID != $_vrid ]]; then
   # logger_error "O_P_VIRTUAL_ROUTER_ID is invalid"
    #return $E_CHK_VAR_INVALID_VARIABLE
  #fi

  logger_debug "checking variable vrrpinstance ..."
  if [[ -z $O_P_VRRPINSTANCE ]]; then
    logger_error "O_P_VRRPINSTANCE is unset"
    return $E_CHK_VAR_INVALID_VARIABLE
  fi
  declare -r _vi="vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"
#echo "_vi=$_vi"
#echo "O_P_VRRPINSTANCE=$O_P_VRRPINSTANCE"
  if [[ ${O_P_DBTYPE} != "pxcw" && ${O_P_DBTYPE} != "pxcr" && ${O_P_DBTYPE} != "mongodb"    ]];then
    if [[ $O_P_VRRPINSTANCE != $_vi ]]; then
      logger_error "O_P_VRRPINSTANCE is invalid"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  fi

  logger_debug "checking variable realservergrp ..."
  if [[ $_dbtype == "oracle" ]];then
    if [[ -n $O_P_REALSERVERGRP && $O_P_REALSERVERGRP != "null" ]]; then
      logger_error "O_P_REALSERVERGRP is not null"
      return $E_CHK_VAR_INVALID_VARIABLE
    fi
  elif [[ $_dbtype == "mysql" || $_dbtype == "lvs" ]]; then
    if [[ $O_P_REALSERVERGRP == "null" ]]; then
      logger_error "O_P_REALSERVERGRP is set to null"
      O_P_REALSERVERGRP=""
    fi
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
# check instance
##
check_instance_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -ri _expected_cnt=$2
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _sidorport=$O_P_SIDORPORT
  declare -i _cnt1 _r1=0
  declare -i _r
  logger_debug "_dbtype=$_dbtype"
  logger_debug "_expected_cnt=$_expected_cnt"
  logger_debug "_vi=$_vi"
  logger_debug "_vip=$_vip"
  logger_debug "_sidorport=$_sidorport"

  logger_debug "counting number of instance ..."
#echo "_dbtype=$_dbtype"
#echo "_vi=$_vi"
#echo "_vip=$_vip"
#echo "_sidorport=$_sidorport"
  if [ -f "$HACONF" ]; then
    if [[ $_dbtype == "pxcr" ]]; then
      _cnt1=$($AWK "{if(  (\$9 ~ /$_vip/||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") &&  (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\")  ) print}" $HACONF | wc -l )
    _r1=$?
    elif [[  $_dbtype == "mongodb" ]]; then
      _cnt1=$($AWK "{if(  (\$9 ~ /$_vip/||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") &&  (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\")  ) print}" $HACONF | wc -l )
    _r1=$?

    else 
    _cnt1=$($AWK "{if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\") ) print}" $HACONF | wc -l)
    _r1=$?
    fi
  else
    _cnt1=0
    _r1=0
    logger_debug "haconf not exist"
  fi
  logger_debug "_cnt1=$_cnt1"
  if [ $_r1 -eq 0 ]; then
    if ((_cnt1 == _expected_cnt)); then
      _r=$E_SUCCESS
    else
      logger_debug "number of instance is not $_expected_cnt: $_cnt1"
      _r=$E_CHK_INS_WRONG_NUMBER
    fi
  else
    logger_error "counting failed"
    _r=$E_CHK_INS_COUNT_ERROR
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}

# this method was changeed to process multi-instance 
check_instance_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -ri _expected_cnt1=$2
  declare -ri _expected_cnt2=$3
  declare -ri _expected_cnt3=$4
  declare -i _cnt1 _cnt2 _cnt3
  declare -i _r1=0 _r2=0 _r3=0
  declare -i _r
  declare -r _vi="$O_P_VRRPINSTANCE"
  declare -r _vs="vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"
  declare -r _vsrv="${O_P_VIP} ${O_P_SIDORPORT}"
  logger_debug "_dbtype=$_dbtype"
  logger_debug "_expected_cnt1=$_expected_cnt1"
  logger_debug "_vi=$_vi"
  logger_debug "_vs=$_vs"
  logger_debug "_vsrv=$_vsrv"

  logger_debug "counting number of instance ..."
  if [[ $_dbtype == "mysql" || $_dbtype == "oracle" ]]; then
    if [ -f "$KEEPALIVED_CONF" ]; then
      _cnt1=$($GREP -i "^[[:space:]]*vrrp_instance[[:space:]]\+${_vi}[[:space:]]*{[[:space:]]*$" $KEEPALIVED_CONF | wc -l)
      _r1=$?
      _cnt2=$($GREP -i "^[[:space:]]*vrrp_script[[:space:]]\+${_vs}[[:space:]]*{[[:space:]]*$" $KEEPALIVED_CONF | wc -l)
      _r2=$?
    else
      logger_debug "keepalived conf not exist"
      _cnt1=0;_cnt2=0
      _r1=0;_r2=0
    fi
    logger_debug "_r1=$_r1 _r2=$_r2"
    logger_debug "_cnt1=$_cnt1 _cnt2=$_cnt2"
    if (( _r1==0 && _r2==0 )); then
      if ((_cnt1==_expected_cnt1 && _cnt2==_expected_cnt2)); then
        _r=$E_SUCCESS
      else
        logger_debug "number of instance is not $_expected_cnt1 $_expected_cnt2: $_cnt1 $_cnt2"
        _r=$E_CHK_INS_WRONG_NUMBER
      fi
    else
      logger_error "counting failed"
      _r=$E_CHK_INS_COUNT_ERROR
    fi
  elif [[ $_dbtype == "lvs" ]]; then
    if [ -f "$KEEPALIVED_CONF" ]; then
      _cnt1=$($GREP -i "^[[:space:]]*vrrp_instance[[:space:]]\+${_vi}[[:space:]]*{[[:space:]]*$" $KEEPALIVED_CONF | wc -l)
      _r1=$?
      _cnt2=$($GREP -i "^[[:space:]]*vrrp_script[[:space:]]\+${_vs}[[:space:]]*{[[:space:]]*$" $KEEPALIVED_CONF | wc -l)
      _r2=$?
      _cnt3=$($GREP -i "^[[:space:]]*virtual_server[[:space:]]\+${_vsrv}[[:space:]]*{[[:space:]]*$" $KEEPALIVED_CONF | wc -l)
      _r3=$?
    else
      _cnt1=0;_cnt2=0;_cnt3=0
      _r1=0;_r2=0;_r3=0
      logger_debug "keepalived conf not exist"
    fi
    logger_debug "_r1=$_r1 _r2=$_r2 _r3=$_r3"
    logger_debug "_cnt1=$_cnt1 _cnt2=$_cnt2 _cnt3=$_cnt3"
    if (( _r1==0 && _r2==0 && _r3==0 )); then
      if ((_cnt1==_expected_cnt1 && _cnt2==_expected_cnt2 && _cnt3==_expected_cnt3)); then
        _r=$E_SUCCESS
      else
        logger_debug "number of instance is not $_expected_cnt1 $_expected_cnt2 $_expected_cnt3: $_cnt1 $_cnt2 $_cnt3"
        _r=$E_CHK_INS_WRONG_NUMBER
      fi
    else
      logger_error "counting failed"
      _r=$E_CHK_INS_COUNT_ERROR
    fi
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}
check_instance()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare -ri _ex_cnt=$3
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="check_instance_${_t}"
    if [ $_t = "ha" ];then
	$_func "$_dbtype" "$_ex_cnt" || \
    { logger_error "check instance failed: $_t $_dbtype"; return $E_CHK_INS_CHECK_INSTANCE; }
    else
        $_func "$_dbtype" "$_ex_cnt" "$_ex_cnt" "$_ex_cnt" || \
        $_func "$_dbtype" 1 1 "$_ex_cnt" || \
        { logger_error "check instance failed: $_t $_dbtype"; return $E_CHK_INS_CHECK_INSTANCE; }
    fi
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
# add instance
##
create_instance_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare _f=$HACONF
  declare _tf=$HACONF_TMP
  logger_debug "writing original file to temp file ..."
  if [ -f "$_f" ]; then
    cat $_f >$_tf || \
    { logger_error "writing original file to temp file failed"; return $E_CRT_INS_WRITE_OLD; }
  fi

  logger_debug "appending new entry to temp file ..."
  declare _entry
  if [[ $_dbtype == "mysql" ]]; then
    _entry="$O_P_MAINTAINANCE${DELIM}mysql${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}${O_P_REALSERVERGRP:-null}"
  elif [[ $_dbtype == "lvs" ]]; then
    _entry="$O_P_MAINTAINANCE${DELIM}lvs${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}${O_P_REALSERVERGRP:-null}"
  elif [[ $_dbtype == "pxcw" ]]; then
    _entry="$O_P_MAINTAINANCE${DELIM}pxcw${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}${O_P_REALSERVERGRP:-null}"
  elif [[ $_dbtype == "pxcr" ]]; then
    # 1,grep pxcr from haconf  2,modify the entry if pxcr exsit in haconf
    declare -i _pxcr_num
    _pxcr_num=$($AWK '{if($2 ~ /pxcr/) print}' $_f | wc -l )
    #echo "99999 _pxcr_num---->$_pxcr_num"
    if [ $_pxcr_num -eq 0 ]; then
      _entry="$O_P_MAINTAINANCE${DELIM}pxcr${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}${O_P_REALSERVERGRP:-null}"
    elif [ $_pxcr_num -eq 1 ];then
      #code
     _orig_vip=$($AWK '{ if($2 ~/pxcr/) print $9 }' $_f) 
     O_P_VIP_NEW=`echo "$_orig_vip,$O_P_VIP"`
    _OLD_O_P_VRRPINSTANCE=$($AWK '{if($2 ~/pxcr/) print $8}' $_f)
    #replace the vrrpinstance name to old name avoid to the magriation issue. 
      _entry="$O_P_MAINTAINANCE${DELIM}pxcr${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$_OLD_O_P_VRRPINSTANCE${DELIM}$O_P_VIP_NEW${DELIM}${O_P_REALSERVERGRP:-null}"
    else
    { logger_error "there are $_pxcr_num  pxcr record in $_f,it should be 0 or 1"return $E_CRT_INS_WRITE_NEW; }

    fi
  elif [[ $_dbtype == "mongodb" ]]; then
    # 1,grep mongodb from haconf  2,modify the entry if mongodb record exsit in haconf
    declare -i _mongodb_num
    _mongodb_num=$($AWK '{if($2 ~ /mongodb/) print}' $_f | wc -l )
    if [ $_mongodb_num -eq 0 ]; then
      _entry="$O_P_MAINTAINANCE${DELIM}mongodb${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}${O_P_REALSERVERGRP:-null}"
    elif [ $_mongodb_num -eq 1 ];then
      #code
     _orig_vip=$($AWK '{ if($2 ~/mongodb/) print $9 }' $_f) 
     O_P_VIP_NEW=`echo "$_orig_vip,$O_P_VIP"`
    _OLD_O_P_VRRPINSTANCE=$($AWK '{if($2 ~/mongodb/) print $8}' $_f)
    #replace the vrrpinstance name to old name avoid to the magriation issue. 
      _entry="$O_P_MAINTAINANCE${DELIM}mongodb${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}null${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$_OLD_O_P_VRRPINSTANCE${DELIM}$O_P_VIP_NEW${DELIM}${O_P_REALSERVERGRP:-null}"
    else
    { logger_error "there are $_mongodb_num  mongodb record in $_f,it should be 0 or 1"return $E_CRT_INS_WRITE_NEW; }

    fi

  elif [[ $_dbtype == "oracle" ]]; then
    _entry="$O_P_MAINTAINANCE${DELIM}oracle${DELIM}$O_P_DBHOME${DELIM}$O_P_SIDORPORT${DELIM}${O_P_DGGROUP}${DELIM}$O_P_DEST_IP${DELIM}$O_P_GOTOFAULT${DELIM}$O_P_VRRPINSTANCE${DELIM}$O_P_VIP${DELIM}null"
  fi

  if [[ $_dbtype == "pxcr" ]];then
    #delete the old pxcr record and add the new one
    $SED -i '/pxcr/'d $_tf
    echo -e "$_entry" >>$_tf || \
    { logger_error "appending new entry to temp file failed"; return $E_CRT_INS_WRITE_NEW; }

  elif [[ $_dbtype == "mongodb" ]];then
    #delete the old mongodb record and add the new one
    $SED -i '/mongodb/'d $_tf
    echo -e "$_entry" >>$_tf || \
    { logger_error "appending new entry to temp file failed"; return $E_CRT_INS_WRITE_NEW; }
	
  else
    echo -e "$_entry" >>$_tf || \
    { logger_error "appending new entry to temp file failed"; return $E_CRT_INS_WRITE_NEW; }

  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


# create keepalived.conf
create_instance_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare _f=$KEEPALIVED_CONF
  declare _tf=$KEEPALIVED_CONF_TMP
  declare -i _vrrp_num=0
  #echo "----Port=$O_P_SIDORPORT"

  logger_debug "writing original file to temp file ..."
  if [ -f "$_f" ]; then
     if [[ ${O_P_DBTYPE} == "pxcr" || ${O_P_DBTYPE} == "mongodb" ]]; then
       _vrrp_num=$($AWK '{if($0 ~ /vrrp_script vs_'"${O_P_DBTYPE}"'/) print}' $_f|wc -l)
       logger_info "_vrrp_num = ${_vrrp_num}"
       cat $_f >$_tf || \
       { logger_error "writing original file to temp file failed"; return $E_CRT_INS_WRITE_OLD; }

     else
       _vrrp_num=$($AWK '{if($0 ~ /vrrp_script vs_'"${O_P_DBTYPE}"'_'"${O_P_VIRTUAL_ROUTER_ID}"'/) print}' $_f|wc -l)
       logger_info "_vrrp_num = ${_vrrp_num}"
       cat $_f >$_tf || \
       { logger_error "writing original file to temp file failed"; return $E_CRT_INS_WRITE_OLD; }
     fi
  fi

  logger_debug "appending new entry to temp file ..."
  if [[ $_dbtype == "mysql" ]]; then
  # here we should check whether the vip is in use
  # if then we will not add configure infos of vrrp_script and vrrp_instance
  # to the keepalived.conf again
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding mysql instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF
vrrp_script vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    script "/etc/keepalived/check_mysql_multi.pl --vrrp-instance=${O_P_VRRPINSTANCE} --total-timeout-seconds=60"
    interval 60
}

vrrp_instance vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 100
    advert_int 5
    authentication {
       auth_type PASS
       auth_pass 9200${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}
    }
    notify "/etc/keepalived/notify.sh"
    virtual_ipaddress {
        $O_P_VIP
    }
}
EOF
     _r=$?
   else
     logger_info "mysql instance with VIP:${O_P_VIP} is already exist"
     logger_info "program will not modified the /etc/keepalived/keepalived.conf for vrrp_instance:${O_P_VIP}"
     _r=0
   fi

######
  elif [[ $_dbtype == "pxcw" ]]; then
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding pxcw instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF
vrrp_script vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    script "/etc/keepalived/check_pxc.pl --vrrp-instance=${O_P_VRRPINSTANCE} --retry-times=3 --total-timeout-seconds=45"
    interval 60
}
vrrp_instance vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 100
    advert_int 5
    authentication {
       auth_type PASS
       auth_pass 9800${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}
    }
    #notify "/etc/keepalived/notify.sh"
    virtual_ipaddress {
        $O_P_VIP
    }
}
EOF
     _r=$?
   else
     logger_info "pxcw instance with VIP:${O_P_VIP} is already exist"
     logger_info "program will not modified the /etc/keepalived/keepalived.conf for vrrp_instance:${O_P_VIP}"
     _r=0
   fi




######

  elif [[ $_dbtype == "oracle" ]]; then
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding oracle instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF
vrrp_script vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    script "/etc/keepalived/check_status_root.sh ${O_P_VRRPINSTANCE}"
    interval 120
}
vrrp_instance vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 100
    advert_int 5
    authentication {
       auth_type PASS
       auth_pass 9800${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}
    }
    notify "/etc/keepalived/notify.sh"
    virtual_ipaddress {
        $O_P_VIP
    }
}
EOF
      _r=$?
    else
      logger_info "oracle instance with VIP:${O_P_VIP} is already exist"
      logger_info "program will not modified the /etc/keepalived/keepalived.conf for vrrp_instance:${O_P_VIP}"
      _r=0
    fi

  elif [[ $_dbtype == "mongodb" ]]; then
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding mongodb instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF
vrrp_script vs_${O_P_DBTYPE} {
    script "/etc/keepalived/check_mongodb.pl --vrrp-instance=${O_P_VRRPINSTANCE} --retry-times=3 --total-timeout-seconds=120"
    interval 120
}
vrrp_instance vi_${O_P_DBTYPE} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 200
    advert_int 5
    authentication {
       auth_type PASS
       auth_pass 9900${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}
    }

    virtual_ipaddress {
EOF
      for _i_1 in ${O_P_VIP//,/ }; do
        cat >>$_tf << EOF
        $_i_1 
EOF
      done

      cat >>$_tf << EOF
    }
}

virtual_server_group mongodb_vip {
EOF
      for _i_2 in ${O_P_VIP//,/ }; do
      cat >>$_tf << EOF
        $_i_2 $O_P_SIDORPORT
EOF
      done
      cat >>$_tf << EOF
}
virtual_server group mongodb_vip {
    delay_loop 15
    lb_algo wlc
    lb_kind DR
    protocol TCP
    ha_suspend
    sorry_server ${O_P_DEST_IP_ARRAY[1]} ${O_P_SIDORPORT}
    real_server ${O_P_DEST_IP_ARRAY[0]} ${O_P_SIDORPORT} {
        weight 3
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_mongodb_real.pl --hostip=${O_P_DEST_IP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
      _r=$?
      for((_i=0;_i<${#O_P_REALSERVERGRP_ARRAY[@]};_i++)); do
        cat >>$_tf <<EOF
    real_server ${O_P_REALSERVERGRP_ARRAY[$_i]} ${O_P_SIDORPORT} {
        weight 5
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_mongodb_real.pl --hostip=${O_P_REALSERVERGRP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
        ((_r=_r+$?))
      done

      cat >>$_tf <<EOF
}
EOF
      ((_r=_r+$?))


    else
      logger_info "other mongodb instance  is already exist"
      logger_info "program will just inserting vip:${O_P_VIP} to exsisting virtual_server group mongodb_vip "
     declare -i _flag
      _flag=$($AWK '
        {
          if($0 ~/^[[:space:]]*vrrp_instance[[:space:]]+vi_mongodb[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
                getline
                if($0 ~/virtual_ipaddress/ )
                {
                    B=1;
                    while (B>0) {
                        getline
                        if ($0 ~ /}/) B--
                        if ($0 ~ /{/) B++
                    }$end of wile B
                   I=0;
                  print NR
                }

            }#end of while I
          }
        }
        ' $_f)
       _r=$?
      #echo "11111111_falg=$_flag"
      $SED -n "1,$((_flag-1))p" $_f>$_tf

      for _i_3 in ${O_P_VIP//,/ }; do
        cat >>$_tf << EOF
        $_i_3
EOF
      done
     #cat >> $_tf <<EOF
    #}
#}
#EOF

#####flag 2 for virtual_server_group mongodb_vip

      declare -i _flag_read_group
      _flag_read_group=$($AWK '
        {
          if($0 ~/^[[:space:]]*virtual_server_group[[:space:]]+mongodb_vip[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
              getline
              if ($0 ~ /}/) I--
              if ($0 ~ /{/) I++
            }#end of while I
            print NR
          }
        }
        ' $_f)
      _r=$?
      #echo "11111111_falgread_group=$_flag_read_group"
      $SED -n "$((_flag)),$((_flag_read_group-1))p" $_f>>$_tf

      #virtual_server_group pxc_read_vip {
      for _i_4 in ${O_P_VIP//,/ }; do
      cat >>$_tf << EOF
        $_i_4 $O_P_SIDORPORT
EOF
      done
    cat >>$_tf << EOF
}
EOF

#####flag 3 for virtual_server group mongodb_vip(real server part)
      declare -i _flag_real_server
      _flag_real_server=$($AWK '
        {
          if($0 ~/^[[:space:]]*virtual_server[[:space:]]+group[[:space:]]+mongodb_vip[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
              getline
              if ($0 ~ /}/) I--
              if ($0 ~ /{/) I++
            }#end of while I
            print NR
          }
        }
        ' $_f)
      _r=$?

      #echo "11111111_falg_read_server=$_flag_real_server"
      cat >>$_tf << EOF

virtual_server group mongodb_vip {
    delay_loop 15
    lb_algo wlc
    lb_kind DR
    protocol TCP
    ha_suspend
    sorry_server ${O_P_DEST_IP_ARRAY[1]} ${O_P_SIDORPORT}
    real_server ${O_P_DEST_IP_ARRAY[0]} ${O_P_SIDORPORT} {
        weight 3
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_mongodb_real.pl --hostip=${O_P_DEST_IP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
      _r=$?

      for((_i=0;_i<${#O_P_REALSERVERGRP_ARRAY[@]};_i++)); do
        cat >>$_tf <<EOF
    real_server ${O_P_REALSERVERGRP_ARRAY[$_i]} ${O_P_SIDORPORT} {
        weight 5
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_mongodb_real.pl --hostip=${O_P_REALSERVERGRP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
        ((_r=_r+$?))
      done
      cat >>$_tf <<EOF
}
EOF
      $SED -n "$((_flag_real_server+1)),\$p" $_f >>$_tf
      ((_r=_r+$?))

    fi
##end mongodb

  elif [[ $_dbtype == "pxcr" ]]; then
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding pxcr instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF
vrrp_script vs_${O_P_DBTYPE} {
    script "/etc/keepalived/check_pxc.pl --vrrp-instance=${O_P_VRRPINSTANCE} --retry-times=3 --total-timeout-seconds=45"
    interval 60
}
vrrp_instance vi_${O_P_DBTYPE} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 200
    advert_int 5
    authentication {
       auth_type PASS
       auth_pass 9900${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}
    }

    virtual_ipaddress {
EOF
      for _i_1 in ${O_P_VIP//,/ }; do
        cat >>$_tf << EOF 
        $_i_1
EOF
      done

      cat >>$_tf << EOF
    }
}

virtual_server_group pxc_read_vip {
EOF
      for _i_2 in ${O_P_VIP//,/ }; do
      cat >>$_tf << EOF 
        $_i_2 $O_P_SIDORPORT
EOF
      done
      cat >>$_tf << EOF
}
virtual_server group pxc_read_vip {
    delay_loop 15
    lb_algo wlc
    lb_kind DR
    protocol TCP
    ha_suspend
    #sorry_server ${O_P_DEST_IP_ARRAY[1]} ${O_P_SIDORPORT}
    real_server ${O_P_DEST_IP_ARRAY[0]} ${O_P_SIDORPORT} {
        weight 3
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_pxc_real.pl --hostip=${O_P_DEST_IP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
      _r=$?
      
      for((_i=0;_i<${#O_P_REALSERVERGRP_ARRAY[@]};_i++)); do
        cat >>$_tf <<EOF
    real_server ${O_P_REALSERVERGRP_ARRAY[$_i]} ${O_P_SIDORPORT} {
        weight 5
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_pxc_real.pl --hostip=${O_P_REALSERVERGRP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
        ((_r=_r+$?))
      done

      cat >>$_tf <<EOF
}
EOF
      ((_r=_r+$?))
      
      
    else
      logger_info "other pxcr instance  is already exist"
      logger_info "program will just inserting vip:${O_P_VIP} to exsisting virtual_server group pxc_read_vip "
#echo ":::::gggg exsit::::"
#echo "O_P_DBTYPE ------->$O_P_DBTYPE"
#echo "O_P_VIRTUAL_ROUTER_ID------->$O_P_VIRTUAL_ROUTER_ID"
      declare -i _flag
      _flag=$($AWK '
        {
          if($0 ~/^[[:space:]]*vrrp_instance[[:space:]]+vi_pxcr[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
                getline
                if($0 ~/virtual_ipaddress/ )
                {
                    B=1;
                    while (B>0) {
                        getline
                        if ($0 ~ /}/) B--
                        if ($0 ~ /{/) B++
                    }$end of wile B
                   I=0;
                  print NR
                }

            }#end of while I
          }
        }
        ' $_f)
       _r=$?
      #echo "11111111_falg=$_flag"
      $SED -n "1,$((_flag-1))p" $_f>$_tf

      for _i_3 in ${O_P_VIP//,/ }; do
        cat >>$_tf << EOF
        $_i_3
EOF
      done
     #cat >> $_tf <<EOF
    #}
#}
#EOF

#####flag 2 for virtual_server_group pxc_read_vip

      declare -i _flag_read_group
      _flag_read_group=$($AWK '
        {
          if($0 ~/^[[:space:]]*virtual_server_group[[:space:]]+pxc_read_vip[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
              getline
              if ($0 ~ /}/) I--
              if ($0 ~ /{/) I++
            }#end of while I
            print NR
          }
        }
        ' $_f)
      _r=$?
      #echo "11111111_falgread_group=$_flag_read_group"
      $SED -n "$((_flag)),$((_flag_read_group-1))p" $_f>>$_tf

      #virtual_server_group pxc_read_vip {
      for _i_4 in ${O_P_VIP//,/ }; do
      cat >>$_tf << EOF
        $_i_4 $O_P_SIDORPORT
EOF
      done
    cat >>$_tf << EOF
}
EOF

#####flag 3 for virtual_server group pxc_read_vip(real server part)
      declare -i _flag_real_server
      _flag_real_server=$($AWK '
        {
          if($0 ~/^[[:space:]]*virtual_server[[:space:]]+group[[:space:]]+pxc_read_vip[[:space:]]*{[[:space:]]*$/)
          {
            I=1;
            while (I>0) {
              getline
              if ($0 ~ /}/) I--
              if ($0 ~ /{/) I++
            }#end of while I
            print NR
          }
        }
        ' $_f)
      _r=$?

     # echo "11111111_falg_read_server=$_flag_real_server"
      cat >>$_tf << EOF

virtual_server group pxc_read_vip {
    delay_loop 15
    lb_algo wlc
    lb_kind DR
    protocol TCP
    ha_suspend
    #sorry_server ${O_P_DEST_IP_ARRAY[1]} ${O_P_SIDORPORT}
    real_server ${O_P_DEST_IP_ARRAY[0]} ${O_P_SIDORPORT} {
        weight 3
        inhibit_on_failure
        MISC_CHECK {
            #misc_path "/etc/keepalived/check_realserver.sh ${O_P_REALSERVERGRP_ARRAY[$_i]} ${O_P_SIDORPORT}"
            misc_path "/etc/keepalived/check_pxc_real.pl --hostip=${O_P_DEST_IP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
      _r=$?

      for((_i=0;_i<${#O_P_REALSERVERGRP_ARRAY[@]};_i++)); do
#echo "O_P_REALSERVERGRP_ARRAY----->${O_P_REALSERVERGRP_ARRAY[$_i]"
        cat >>$_tf <<EOF
    real_server ${O_P_REALSERVERGRP_ARRAY[$_i]} ${O_P_SIDORPORT} {
        weight 5
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_pxc_real.pl  --hostip=${O_P_REALSERVERGRP_ARRAY[$_i]} --port=${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
        ((_r=_r+$?))
      done

      cat >>$_tf <<EOF
}
EOF
      $SED -n "$((_flag_real_server+1)),\$p" $_f >>$_tf
      ((_r=_r+$?))

    fi
##end pxcr

  elif [[ $_dbtype == "lvs" ]]; then
    if [ $_vrrp_num -eq 0 ]; then
      logger_info "adding lvs instance with a new VIP:${O_P_VIP}"
      cat >>$_tf <<EOF

vrrp_script vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    script "/etc/keepalived/check_lvs_multi.pl --vrrp-instance=vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} --total-timeout-seconds=60"
    interval 60
}

vrrp_instance vi_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID} {
    state BACKUP
    nopreempt
    interface $O_VIP_INTERFACE
    virtual_router_id $O_P_VRID
    priority 200
    advert_int 2
    authentication {
       auth_type PASS
       auth_pass 9300${O_P_VRID}
    }
    track_script {
        vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}
    }
    virtual_ipaddress {
        $O_P_VIP
    }
}
EOF
      _r=$?
    else
      logger_info "lvs instance with vip:${O_P_VIP} is already exist"
      logger_info "program will not modified the /etc/keepalived/keepalived.conf for vrrp_instance:${O_P_VIP}"
      _r=0
    fi
  fi

  if [ $_r -ne 0 ]; then
    logger_error "appending new entry to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
##end lvs
}


create_instance()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="create_instance_${_t}"
    $_func "$_dbtype" || \
    { logger_error "create instance failedL $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
add_instance()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_info "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  # added later
  # for multi_instance purpose
  # here we add port check
  O_SIDORPORT=${O_SIDORPORT:-$O_P_SIDORPORT}
  O_SIDORPORT=${O_SIDORPORT:-3306}
  
  O_P_SIDORPORT=$O_SIDORPORT
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb"  ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi

  logger_info "setting default variables ..."
  set_default_variables "$O_DBTYPE"

  logger_info "checking validation of variables ..."
  check_variables "$O_DBTYPE" || \
  { logger_error "checking validation of variables failed"; print_variables "INFO";  exit $E_I_INVALID_VARIABLE; }
  print_variables

  #O_VRRPINSTANCE=$O_P_VRRPINSTANCE
  #O_VIP=$O_P_VIP
  logger_info "checking existence of vrrp instance ..."
  if ! check_instance "$O_TARGET" "$O_DBTYPE" 0; then
    if [[ $O_FLAG_FORCE == "Y" ]]; then
      logger_warn "vrrp instance already exists"
    else
      logger_error "vrrp instance already exists"
      exit $E_I_NO_INSTANCE
    fi
  fi

  logger_info "backuping conf ..."
  backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

  logger_info "creating a new vrrp instance ..."
  create_instance "$O_TARGET" "$O_DBTYPE"
  if [ $? -ne 0 ]; then
    logger_error "creating a new vrrp instance failed"
    logger_info "removing backup conf ..."
    restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
    exit $E_I_CREATE_INSTANCE
  fi
  logger_info "add instance finished"

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
# delete instance
##
drop_instance_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _sidorport=$O_P_SIDORPORT
  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP
  declare -i _r
  logger_debug "removing entry ..."
  declare _entry
  if [ -f "$_f" ]; then
    if [[ $_dbtype == "pxcr" ]];then
       _pxcr_num=$($GREP "$_dbtype"  $_f | $GREP "$_vip" |  $GREP -v " $GREP" | wc -l )
      echo " ----->pxcr_vip=$_vip"
      echo " ----->pxcr_num_hacon=$_pxcr_num"
      if [[ $_pxcr_num == 1 ]];then 
       $AWK "{if(  ( \$2==\"$_dbtype\" ) ); else print}"  $_f >$_tf 
       #modify the pxcr record: 1,delete vip form this record 2,add the pxcr record to $_tf
       if [ $? -ne 0 ]; then
         logger_error "removing entry failed"
         return $E_CRT_INS_WRITE_NEW
       fi
       
      fi
    else
      $AWK "{if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\" || \"$_sidorport\"==\"\")) ;else print}" $_f >$_tf
      if [ $? -ne 0 ]; then
        logger_error "removing entry failed"
        return $E_CRT_INS_WRITE_NEW
      fi
    fi
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# only when tha last mysql instance of an vip is droped, can the vrrp_instance be deleted
# only when the last lvs instance of an vip is droped, can the vrrp_instance be deleted
drop_instance_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r=0
  declare -i _i
  declare -i _num_ins=0
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _sidorport=$O_P_SIDORPORT
  declare -r _haconf=$HACONF
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP

  # the haconf
  #[ -f $_haconf ] && _num_ins=$($AWK "{if ((\$8==\"$_vi\" || \"$_vi\"==\"\") && (\$9==\"$_vip\" || \"$_vip\"==\"\") && \$4!=\"$_sidorport\") print}" |wc -l) || {logger_error "read haconf error when dropping instance in keepalived"; return $E_CRT_INS_WRITE_NEW}
  logger_debug "removing entry ..."
  if [[ $_dbtype == "mysql" || $_dbtype == "oracle" || $_dbtype == "pxcw"  ]]; then
    if [ ! -s $_haconf ];then
      _num_ins=0
    else
      [ -f $_haconf ] && _num_ins=$($AWK "{if ((\$8==\"$_vi\" || \"$_vi\"==\"\") && (\$9==\"$_vip\" || \"$_vip\"==\"\") && \$4!=\"$_sidorport\") print}" $_haconf|wc -l) || { logger_error "read haconf error when dropping instance in keepalived"; return $E_CRT_INS_WRITE_NEW; }
    fi
    logger_info "_num_ins = ${_num_ins}"
    #logger_info "there is no db instance running on VIP:${_vip}, program will remove the vrrp_instance in keepalived.conf ..."
    if [ $_num_ins -eq 0 ];then
      logger_info "there is no db instance running on VIP:${O_P_VIP}, program will remove the vrrp_instance in keepalived.conf ..."
      $AWK '
{
  if ( ($0 ~ /^[[:space:]]*vrrp_instance[[:space:]]+'"$O_P_VRRPINSTANCE"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*vrrp_script[[:space:]]+'"vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
  }
  else
    print
}
' $_f >$_tf
      _r=$?
    else
      logger_info "there is still ${_num_ins} db instance running on VIP:${O_P_VIP}, program will not remove the vrrp_instance in keepalived.conf"
      cat $_f >$_tf
      _r=0
    fi
  elif [[ $_dbtype == "lvs" ]]; then
    _num_ins=$($AWK '{if ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'/) print}' $_f |$GREP -v "${O_P_SIDORPORT}"|wc -l)
    if [ $_num_ins -eq 0 ];then
      logger_info "there is no lvs instance running on VIP:${O_P_VIP}, program will remove the vrrp_instance in keepalived.conf"
      $AWK '
{
  if ( ($0 ~ /^[[:space:]]*vrrp_instance[[:space:]]+'"${O_P_VRRPINSTANCE}"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*vrrp_script[[:space:]]+'"vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
  }
  else
    print
}
' $_f >$_tf
      _r=$?
    else
      logger_info "there is still ${_num_ins} lvs instance running on VIP:${O_P_VIP}, program will not remove the vrrp_instance in keepalived.conf"
      $AWK '
{
  if($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/)
  {
    I=1;
    while (I>0) {
      getline
      if ($0 ~/}/) I--
      if ($0 ~/{/) I++
    }
  }
  else
    print
}
' $_f >$_tf
      _r=$?
    fi

  elif [[ $_dbtype == "pxcr" ]];then
    #for pxcr delete vrrpinstance from keepalived
    echo " ---------delete pxcr"
  fi
  if [ $_r -ne 0 ]; then
    logger_error "appending new entry to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
drop_instance()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="drop_instance_${_t}"
    $_func "$_dbtype" || \
    { logger_error "drop instance failedL $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
delete_instance()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_info "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"

  # added for multi-instance
  O_SIDORPORT=${O_P_SIDORPORT:-$O_SIDORPORT}
  O_SIDORPORT=${O_SIDORPORT:-3306}

  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE
  O_P_SIDORPORT=$O_SIDORPORT
  logger_info "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    if [[ $O_FLAG_FORCE == "Y" ]]; then
      logger_info "instance not found"
      return
    else
      logger_error "haconf invalid"
      exit $E_I_READ_HACONF
    fi
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  logger_info "setting default variables ..."
  set_default_variables "$O_DBTYPE"

  logger_info "checking validation of variables ..."
  check_variables "$O_DBTYPE" || \
  { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
  print_variables

  logger_info "checking existence of vrrp instance ..."
  if ! check_instance "$O_TARGET" "$O_DBTYPE" 1; then
    if [[ $O_FLAG_FORCE == "Y" ]]; then
      logger_warn "vrrp instance not exists"
    else
      logger_error "vrrp instance not exists"
      exit $E_I_NO_INSTANCE
    fi
  fi

  logger_info "backuping conf ..."
  backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

  logger_info "dropping vrrp instance ..."
  drop_instance "$O_TARGET" "$O_DBTYPE"
  if [ $? -ne 0 ]; then
    logger_error "dropping vrrp instance failed"
    logger_info "removing backup conf ..."
    restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
    exit $E_I_CREATE_INSTANCE
  fi
  logger_info "drop instance finished"

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
# add realserver
##
create_realserver_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _sidorport=$O_P_SIDORPORT
  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP

  logger_debug "editing entry ..."
  declare _entry
  if [ -f "$_f" ]; then
    $AWK "BEGIN{OFS=\"${DELIM}\"} {if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\")) \$10=\"${O_P_REALSERVERGRP:-null}\"; print}" $_f >$_tf
    if [ $? -ne 0 ]; then
      logger_error "editing entry failed"
      return $E_CRT_INS_WRITE_NEW
    fi
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
create_realserver_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP
  declare -r _tf2=$KEEPALIVED_CONF_TMP2
  declare -i _flag

  logger_debug "editing entry ..."

  if [[ $_dbtype == "pxcr" ]];then
    _flag=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+group[[:space:]]+pxc_read_vip[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
  }
}' $_f)
    if [ $? -ne 0 ]; then
      logger_error "appending new entry to temp file failed"
      return $E_CRT_INS_WRITE_NEW
    fi
  
  elif [[ $_dbtype == "mongodb" ]];then
    #for mysql lvs pxcw instance
    _flag=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+group[[:space:]]+mongodb_vip[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
  }
}' $_f)
    if [ $? -ne 0 ]; then
      logger_error "appending new entry to temp file failed"
      return $E_CRT_INS_WRITE_NEW
    fi
 
  else
    #for mysql lvs pxcw instance
    _flag=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
  }
}' $_f)
    if [ $? -ne 0 ]; then
      logger_error "appending new entry to temp file failed"
      return $E_CRT_INS_WRITE_NEW
    fi
    
  fi

  logger_debug "_flag=$_flag"
  if [ $_flag -le 1 ]; then
    logger_error "flag not found"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing part1 to temp file ..."
  $SED -n "1,$((_flag-1))p" $_f >$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing 1st part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing real server to temp file ..."
  logger_debug "#O_REALSERVER_ARRAY=${#O_REALSERVER_ARRAY[@]}"
  for((_i=0;_i<${#O_REALSERVER_ARRAY[@]};_i++)); do
    logger_debug "adding realserver: ${O_REALSERVER_ARRAY[$_i]} ..."
    cat >>$_tf <<EOF
    real_server ${O_REALSERVER_ARRAY[$_i]} ${O_P_SIDORPORT} {
        weight $O_P_VSRV_WEIGHT
        inhibit_on_failure
        MISC_CHECK {
            misc_path "/etc/keepalived/check_realserver.sh ${O_REALSERVER_ARRAY[$_i]} ${O_P_SIDORPORT}"
            misc_timeout 15
            misc_dynamic
        }
    }
EOF
    if [ $? -ne 0 ]; then
      logger_error "appending new entry to temp file failed"
      return $E_CRT_INS_WRITE_NEW
    fi
  done

  logger_debug "writing part2 to temp file ..."
  $SED -n "$_flag,\$p" $_f >>$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing 2nd part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
create_realserver()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="create_realserver_${_t}"
    if [[ $_dbtype == "mysql" && $_t == "keepalived" ]]; then
      true
    else
      $_func "$_dbtype" || \
      { logger_error "create realserver failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
    fi
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
add_realserver()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_info "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE

  if [[ -z "$O_REALSERVER" ]]; then
    logger_error "O_REALSERVER is null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  set_realserver_array

  logger_info "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    logger_error "haconf invalid"
    exit $E_I_READ_HACONF
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  logger_info "setting default variables ..."
  set_default_variables "$O_DBTYPE"

  logger_info "analyzing realserver ..."
  declare _new_realserver
  declare -i _i _j
  for((_i=0; _i<${#O_REALSERVER_ARRAY[@]}; _i++)); do
    declare _rs=${O_REALSERVER_ARRAY[$_i]}
    if in_array "$_rs" "${O_P_REALSERVERGRP_ARRAY[@]}"; then
      logger_debug "realserver already exists: $_rs"
    else
      _new_realserver="$_new_realserver,$_rs"
    fi
  done
  _new_realserver=${_new_realserver/#,/}
  if [[ $_new_realserver != $O_REALSERVER ]]; then
    logger_debug "set new realserver"
    O_REALSERVER=$_new_realserver
    set_realserver_array
  fi
  logger_debug "O_REALSERVER=$O_REALSERVER"

  if [[ -n $O_REALSERVER ]]; then
    logger_info "analyzing realservergrp ..."
    if [[ -z $O_P_REALSERVERGRP || $O_P_REALSERVERGRP == "null" ]]; then
      O_P_REALSERVERGRP=$O_REALSERVER
    else
      O_P_REALSERVERGRP="${O_P_REALSERVERGRP},$O_REALSERVER"
    fi
    set_realservergrp_array

    logger_info "checking validation of variables ..."
    check_variables "$O_DBTYPE" || \
    { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
    print_variables

    logger_info "checking existence of vrrp instance ..."
    check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
    { logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

    logger_info "backuping conf ..."
    backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

    logger_info "adding real server ..."
    create_realserver "$O_TARGET" "$O_DBTYPE"
    if [ $? -ne 0 ]; then
      logger_error "adding real server failed"
      logger_info "removing backup conf ..."
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_CREATE_INSTANCE
    fi
    logger_info "add realserver finished"

  else
    logger_info "add real server not needed"
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# real_server add process for multi-instance
add_realserver_scheduler()
{
  logger_info "entering function ${FUNCNAME[0]}()..."
  logger_info "processing sidorport ..."
  
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  for _t in ${O_TARGET//,/ };do
    if [[ $_t != "ha" && $_t != "keepalived" ]];then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  
  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]];then
    logger_error "parameter VRRPINSTANCE and VIP are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  if [[ -z $O_REALSERVER ]];then
    logger_error "the realserver you want to add is null,program will exit..."
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  
  declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE

  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be added to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  #sidorports=${O_SIDORPORT:-$O_P_SIDORPORT} 

  logger_info "processing the realserver add on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."
    
    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    O_REALSERVER=$_temp_realserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port
    
    add_realserver
  done
  
  logger_info "leaving function $FUNCNAME[0]()..."
}

##
# delete realserver
##
drop_realserver_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _sidorport=$O_P_SIDORPORT
  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP

  logger_debug "editing entry ..."
  declare _entry
  if [ -f "$_f" ]; then
    $AWK "BEGIN{OFS=\"${DELIM}\"} {if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$O_P_SIDORPORT\"||\"$O_P_SIDORPORT\"==\"\")) \$10=\"${O_P_REALSERVERGRP:-null}\"; print}" $_f >$_tf || \
    { logger_error "editing entry failed"; return $E_CRT_INS_WRITE_NEW; }
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
drop_realserver_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP
  declare -r _tf2=$KEEPALIVED_CONF_TMP2
  declare -i _flag_begin _flag_end

  logger_debug "finding virtual server ..."
  _flag_begin=$($AWK '/^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/{print NR;exit}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_begin=$_flag_begin"
  if [[ $_flag_begin -eq 0 ]]; then
    logger_error "flag begin not found"
    return $E_CRT_INS_WRITE_NEW
  fi
  _flag_end=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
    exit
  }
}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin end failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_end=$_flag_end"
  if [ $_flag_end -le 1 ]; then
    logger_error "flag end not found"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing virtual server part to temp file ..."
  $SED -n "${_flag_begin},${_flag_end}p" $_f >$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing virtual server part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "deleting realservers in virtual server ..."
  logger_debug "#O_REALSERVER_ARRAY=${#O_REALSERVER_ARRAY[@]}"
  for((_i=0;_i<${#O_REALSERVER_ARRAY[@]};_i++)); do
    logger_debug "deleting realserver: ${O_REALSERVER_ARRAY[$_i]} ..."
    $AWK '
{
  if ( ($0 ~ /^[[:space:]]*real_server[[:space:]]+'"${O_REALSERVER_ARRAY[$_i]}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
  }
  else
    print
}
' $_tf >$_tf2 && $MV -f $_tf2 $_tf
    if [ $? -ne 0 ]; then
      logger_error "deleting entry failed: ${O_REALSERVER_ARRAY[$_i]}"
      return $E_CRT_INS_WRITE_NEW
    fi
  done

  logger_debug "combining 3 parts to temp file ..."
  $SED -n "1,$((_flag_begin-1))p" $_f >$_tf2 && \
  $CAT $_tf >>$_tf2 && \
  $SED -n "$((_flag_end+1)),\$p" $_f >>$_tf2
  if [ $? -ne 0 ]; then
    logger_error "combining 3 parts to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf2" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
drop_realserver()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="drop_realserver_${_t}"
    if [[ $_dbtype == "mysql" && $_t == "keepalived" ]]; then
      true
    else
      logger_debug "$_func $_dbtype"
      $_func "$_dbtype" || \
      { logger_error "drop realserver failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
    fi
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
delete_realserver()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_info "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE

  if [[ -z "$O_REALSERVER" ]]; then
    logger_error "O_REALSERVER is null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  set_realserver_array

  logger_info "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    logger_error "haconf invalid"
    exit $E_I_READ_HACONF
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  logger_info "setting default variables ..."
  set_default_variables "$O_DBTYPE"

  logger_info "analyzing realserver ..."
  declare _new_realserver
  declare -i _i _j
  for((_i=0; _i<${#O_REALSERVER_ARRAY[@]}; _i++)); do
    declare _rs=${O_REALSERVER_ARRAY[$_i]}
    if in_array "$_rs" "${O_P_REALSERVERGRP_ARRAY[@]}"; then
      _new_realserver="$_new_realserver,$_rs"
    else
      logger_debug "realserver not exists: $_rs"
    fi
  done
  _new_realserver=${_new_realserver/#,/}
  if [[ $_new_realserver != $O_REALSERVER ]]; then
    logger_debug "set new realserver"
    O_REALSERVER=$_new_realserver
    set_realserver_array
  fi
  logger_debug "O_REALSERVER=$O_REALSERVER"

  if [[ -n $O_REALSERVER ]]; then
    logger_info "analyzing realservergrp ..."
    declare _new_realservergrp
    declare -i _i _j
    for((_i=0; _i<${#O_P_REALSERVERGRP_ARRAY[@]}; _i++)); do
      declare _rsg=${O_P_REALSERVERGRP_ARRAY[$_i]}
      if in_array "$_rsg" "${O_REALSERVER_ARRAY[@]}"; then
        logger_debug "realservergrp exists: $_rsg"
      else
        _new_realservergrp="$_new_realservergrp,$_rsg"
      fi
    done
    _new_realservergrp=${_new_realservergrp/#,/}
    if [[ $_new_realservergrp != $O_P_REALSERVERGRP ]]; then
      logger_debug "set new realservergrp"
      O_P_REALSERVERGRP=$_new_realservergrp
      set_realservergrp_array
    fi
    logger_debug "O_P_REALSERVERGRP=$O_P_REALSERVERGRP"

    logger_info "checking validation of variables ..."
    check_variables "$O_DBTYPE" || \
    { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
    print_variables

    logger_info "checking existence of vrrp instance ..."
    check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
    { logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

    logger_info "backuping conf ..."
    backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

    logger_info "deleting real server ..."
    drop_realserver "$O_TARGET" "$O_DBTYPE"
    if [ $? -ne 0 ]; then
      logger_error "deleting real server failed"
      logger_info "removing backup conf ..."
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_CREATE_INSTANCE
    fi
    logger_info "delete realserver finished"

  else
    logger_info "delete real server not needed"
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# added for realserver delete when multi-instance is running on one vip
delete_realserver_scheduler()
{
  logger_debug "entering function $FUNCNAME[0]()..."
  
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  if [[ -z "$O_REALSERVER" ]]; then
    logger_error "the realserver you want to delete is null,program will exit..."
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE

  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be added to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  
  #for _port in ${_sidorport//,/ };do
  #logger_info "^^^^^^^^^^^^^^^^^^^^^^$_sidorports^^^^^^^^^^^^^^^^^^^^"
  logger_info "processing the realserver add on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."

    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    O_REALSERVER=$_temp_realserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port

    delete_realserver
  done
    
  logger_debug "leaving function $FUNCNAME[0]()..."
}

##
# edit realserver conf
##
edit_realserver2_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP
  declare -r _tf2=$KEEPALIVED_CONF_TMP2
  declare -i _flag_begin _flag_end

  logger_debug "finding virtual server ..."
  _flag_begin=$($AWK '/^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/{print NR;exit}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_begin=$_flag_begin"
  if [[ $_flag_begin -eq 0 ]]; then
    logger_error "flag begin not found"
    return $E_CRT_INS_WRITE_NEW
  fi
  _flag_end=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
    exit
  }
}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin end failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_end=$_flag_end"
  if [ $_flag_end -le 1 ]; then
    logger_error "flag end not found"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing virtual server part to temp file ..."
  $SED -n "${_flag_begin},${_flag_end}p" $_f >$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing virtual server part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "editing realservers in virtual server ..."
  logger_debug "#O_REALSERVER_ARRAY=${#O_REALSERVER_ARRAY[@]}"
  for((_i=0;_i<${#O_REALSERVER_ARRAY[@]};_i++)); do
    logger_debug "editing realserver: ${O_REALSERVER_ARRAY[$_i]} ..."
    $AWK '
{
  if ( ($0 ~ /^[[:space:]]*real_server[[:space:]]+'"${O_REALSERVER_ARRAY[$_i]}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    print
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
      if ($0 ~ /^[[:space:]]*weight[[:space:]]+[[:digit:]]+[[:space:]]*$/) {
        $0="        weight '"$O_P_VSRV_WEIGHT"'"
      }
      print
    } #end of while I
  }
  else
    print
}
' $_tf >$_tf2 && $MV -f $_tf2 $_tf
    if [ $? -ne 0 ]; then
      logger_error "editing entry failed: ${O_REALSERVER_ARRAY[$_i]}"
      return $E_CRT_INS_WRITE_NEW
    fi
  done

  logger_debug "combining 3 parts to temp file ..."
  $SED -n "1,$((_flag_begin-1))p" $_f >$_tf2 && \
  $CAT $_tf >>$_tf2 && \
  $SED -n "$((_flag_end+1)),\$p" $_f >>$_tf2
  if [ $? -ne 0 ]; then
    logger_error "combining 3 parts to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf2" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
edit_realserver2()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    # added for avoiding mitakenlly calling methods dose not exist
    if [[ $_t == "ha" ]];then
      continue
    fi

    declare _func="edit_realserver2_${_t}"
    $_func "$_dbtype" || \
    { logger_error "edit realserver 2 failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
edit_realserver()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_debug "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-keepalived}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE

  if [[ -z "$O_REALSERVER" ]]; then
    logger_error "O_REALSERVER is null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  set_realserver_array

  logger_debug "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    logger_error "haconf invalid"
    exit $E_I_READ_HACONF
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  set_default_variables "$O_DBTYPE"

  logger_debug "parsing realserver ..."
  declare _new_realserver
  declare -i _i _j
  for((_i=0; _i<${#O_REALSERVER_ARRAY[@]}; _i++)); do
    declare _rs=${O_REALSERVER_ARRAY[$_i]}
    if in_array "$_rs" "${O_P_REALSERVERGRP_ARRAY[@]}"; then
      _new_realserver="$_new_realserver,$_rs"
    else
      logger_debug "realserver not exists: $_rs"
    fi
  done
  _new_realserver=${_new_realserver/#,/}
  if [[ $_new_realserver != $O_REALSERVER ]]; then
    logger_debug "set new realserver"
    O_REALSERVER=$_new_realserver
    set_realserver_array
  fi
  logger_debug "O_REALSERVER=$O_REALSERVER"

  if [[ -n $O_REALSERVER ]]; then
    logger_info "checking validation of variables ..."
    check_variables "$O_DBTYPE" || \
    { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
    print_variables

    logger_info "checking existence of vrrp instance ..."
    check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
    { logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

    logger_info "backuping conf ..."
    backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

    logger_info "editing real server variables..."
    edit_realserver2 "$O_TARGET"
    if [ $? -ne 0 ]; then
      logger_error "editing real server variables failed"
      logger_info "removing backup conf ..."
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_CREATE_INSTANCE
    fi

  else
    logger_debug "editing real server not needed"
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# added for multi-instance process
edit_realserver_scheduler()
{
  logger_debug "entering function ${FUNCNAME[0]}()..."
  logger_debug "processing sidorport..."
  
  O_TARGET=${O_TARGET:-keepalived}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  for _t in ${O_TARGET//,/ };do
    if [[ $_t != "ha" && $_t != "keepalived" ]];then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  
  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]];then
    logger_error "parameter VRRPINSTANCE and VIP are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  if [[ -z $O_REALSERVER ]];then
    logger_error "the realserver you want to edit is null,program will exit..."
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  
  declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE

  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be edited to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  #sidorports=${O_SIDORPORT:-$O_P_SIDORPORT} 

  logger_info "processing the realserver add on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."
    
    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    O_REALSERVER=$_temp_realserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port
    
    edit_realserver
  done
  
  logger_info "leaving function ${FUNCNAME[0]}()..."
}
##
# edit ha conf
##
edit_ha2_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_VRRPINSTANCE
  declare -r _vip=$O_VIP
  declare -r _sidorport=$O_SIDORPORT
  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP

  logger_debug "editing ha entry ..."
  $AWK '
function nvl(s,t,   r)
{
  if(s=="") {
    if(t=="")
      r="null"
    else
     r=t
  } else
    r=s
  return r
}
BEGIN {
  OFS="'"${DELIM}"'"
  c_vi=tolower("'"$_vi"'")
  c_vip=tolower("'"$_vip"'")
  c_dbtype=tolower("'"$_dbtype"'")
  c_sidorport="'"$_sidorport"'"
  p["maintainance"]=1
  p["dbtype"]=2
  p["dbhome"]=3
  p["sidorport"]=4
  p["dggroup"]=5
  p["dest_ip"]=6
  p["gotofault"]=7
  p["vrrpinstance"]=8
  p["vip"]=9
  p["realservergrp"]=10
  v["maintainance"]="'"$O_P_MAINTAINANCE"'"
  v["dbtype"]="'"$O_P_DBTYPE"'"
  v["dbhome"]="'"$O_P_DBHOME"'"
  v["sidorport"]="'"$O_P_SIDORPORT"'"
  v["dggroup"]="'"$O_P_DGGROUP"'"
  v["dest_ip"]="'"$O_P_DEST_IP"'"
  v["gotofault"]="'"$O_P_GOTOFAULT"'"
  v["vrrpinstance"]="'"$O_P_VRRPINSTANCE"'"
  v["vip"]="'"$O_P_VIP"'"
  v["realservergrp"]="'"$O_P_REALSERVERGRP"'"
}
{
  if ( (tolower($p["vrrpinstance"])==c_vi||c_vi=="") && (tolower($p["vip"])==c_vip||c_vip=="") && (tolower($p["dbtype"])==c_dbtype||c_dbtype=="") && (tolower($p["sidorport"])==c_sidorport||c_sidorport=="") )
  {
    for (var in v) {
      if (v[var]!="") {
        $p[var]=nvl(v[var],"null")
      }
    }
  }
  print
}' $_f >$_tf || \
  { logger_error "editing ha entry failed"; return $E_CRT_INS_WRITE_NEW; }

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
edit_ha2()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="edit_ha2_${_t}"
    $_func "$_dbtype" || \
    { logger_error "edit ha 2 failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
edit_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_debug "checking parameters ..."
  O_TARGET=${O_TARGET:-ha}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_TARGET=$O_TARGET"
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  #O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  #O_P_VIP=$O_VIP
  #O_P_DBTYPE=$O_DBTYPE

  #logger_debug "reading haconf ..."
  #if ! check_instance_ha "$O_DBTYPE" 1; then 
  #  logger_error "haconf invalid"
  #  exit $E_I_READ_HACONF
  #fi
  #read_variables_in_haconf

  #O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  #logger_debug "O_DBTYPE=$O_DBTYPE"
  #if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" ]]; then
  #  logger_error "invalid dbtype: $O_DBTYPE"
  #  exit $E_I_INVALID_DBTYPE
  #fi
  #set_default_variables "$O_DBTYPE"

  #logger_info "checking validation of variables ..."
  #check_variables "$O_DBTYPE" || \
  #{ logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
  #print_variables

  #logger_info "checking existence of vrrp instance ..."
  #check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
  #{ logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

  logger_info "backuping conf ..."
  backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

  logger_info "editing ha variables..."
  edit_ha2 "$O_TARGET" "$O_DBTYPE"
  if [ $? -ne 0 ]; then
    logger_error "editing ha variables failed"
    logger_info "removing backup conf ..."
    restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
    exit $E_I_CREATE_INSTANCE
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# added for multi-instance
edit_ha_scheduler()
{
  logger_debug "entering function ${FUNCNAME[0]}()..."
  logger_debug "processing sidorport..."
  
  O_TARGET=${O_TARGET:-ha}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  for _t in ${O_TARGET//,/ };do
    if [[ $_t != "ha" && $_t != "keepalived" ]];then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  
  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]];then
    logger_error "parameter VRRPINSTANCE and VIP are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  #if [[ -z $O_REALSERVER ]];then
  #  logger_error "the realserver you want to add is null,program will exit..."
  #  exit $E_I_INVALID_VRRPINSTANCE
  #fi
  
  #declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE

  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be added to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  
  if [[ -z _sidorport ]];then
    logger_error "there is no instance running on VIP:${O_VIP} or VRRPINSTANCE:${O_VRRPINSTANCE}..."
    logger_error "program will exit"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  #sidorports=${O_SIDORPORT:-$O_P_SIDORPORT} 

  logger_info "processing the ha-instance on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."
    
    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    O_REALSERVER=$_temp_realserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port
    
    edit_ha
  done
  
  logger_info "leaving function ${FUNCNAME[0]}()..."
}
##
# auto create keepalived.conf.
##
create_keepalived_conf()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  declare _recreate=$1

  declare _vi
  declare _sidorport
  O_TARGET="keepalived"

  logger_debug "reading haconf ..."
  if [ ! -f "$HACONF" ]; then
    logger_error "haconf not found: $HACONF"
    exit $E_I_CREATE_INSTANCE
  fi

  logger_info "backuping conf ..."
  backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

  if [[ $_recreate == "recreate" ]]; then
    logger_info "removing ka conf ..."
    remove_conf "keepalived"
  fi

  while read _vi _sidorport _vip _dbtype; do
    logger_debug "_vi=$_vi _sidorport=$_sidorport"
    logger_info "adding instance $_vi $_sidorport ..."
    O_VRRPINSTANCE=$_vi
    O_VIP=$_vip
    O_DBTYPE=$_dbtype
    O_SIDORPORT=$_sidorport
    O_P_VRRPINSTANCE=$_vi
    O_P_VIP=$_vip
    O_P_DBTYPE=
    O_P_SIDORPORT=$_sidorport
    O_P_VRID=""

    read_variables_in_haconf
    if ! check_instance_ha "$O_DBTYPE" 1; then 
      logger_error "haconf invalid"
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_READ_HACONF
    fi

    O_DBTYPE=${O_P_DBTYPE}
    logger_debug "O_DBTYPE=$O_DBTYPE"
    if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
      logger_error "invalid dbtype: $O_DBTYPE"
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_INVALID_DBTYPE
    fi
    set_default_variables "$O_DBTYPE"

    logger_info "checking validation of variables ..."
    if ! check_variables "$O_DBTYPE"; then
      logger_error "checking validation of variables failed"
      print_variables "INFO"
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_INVALID_VARIABLE
    fi
    print_variables
    logger_info "checking existence of vrrp instance ..."
    if check_instance "$O_TARGET" "$O_DBTYPE" 0; then
      logger_info "creating a new vrrp instance ..."
      create_instance "$O_TARGET" "$O_DBTYPE"
      if [ $? -ne 0 ]; then
        logger_error "creating a new vrrp instance failed"
        logger_info "removing backup conf ..."
        restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
        exit $E_I_CREATE_INSTANCE
      fi
      
      logger_info "adding realserver ..."
      O_REALSERVER=$O_P_REALSERVERGRP
      if [ ! -z $O_REALSERVER ];then
        add_realserver
      fi
      if [ $? -ne 0 ]; then
        logger_error "adding realserver  failed"
        logger_info "removing backup conf ..."
        restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
        exit $E_I_CREATE_INSTANCE
      fi

    elif check_instance "$O_TARGET" "$O_DBTYPE" 1; then
      logger_info "vrrp instance already exists ..."

    else
      logger_error "more than 1 vrrp instance exists"
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_NO_INSTANCE
    fi

  done < <($SED '/^#/d;/^[[:space:]]*$/d' $HACONF | $AWK '{print $8, $4, $9,$2}')

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
#
##
show_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$O_P_DBTYPE
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  declare -r _port=$O_P_PORT
  declare -r _f=$HACONF
  if [ -f "$_f" ]; then
    $AWK "{if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_port\"||\"$_port\"==\"\")  ) print}" $_f
    if [ $? -ne 0 ]; then
      logger_error "show ha failed"
      return $E_CRT_INS_WRITE_NEW
    fi
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
show_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$O_P_DBTYPE
  declare -i _r=1
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF

  if [[ $_dbtype == "mysql" || $_dbtype == "oracle" ]]; then
    $AWK '
{
  if ( ($0 ~ /^[[:space:]]*vrrp_instance[[:space:]]+'"$O_P_VRRPINSTANCE"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*vrrp_script[[:space:]]+'"vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      print
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print
  }
}
' $_f
    _r=$?
  elif [[ $_dbtype == "lvs" ]]; then
    $AWK '
{
  if ( ($0 ~ /^[[:space:]]*vrrp_instance[[:space:]]+'"$O_P_VRRPINSTANCE"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*vrrp_script[[:space:]]+'"vs_${O_P_DBTYPE}_${O_P_VIRTUAL_ROUTER_ID}"'[[:space:]]*{[[:space:]]*$/) || ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      print
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print
  }
}
' $_f
    _r=$?
  fi
  if [ $_r -ne 0 ]; then
    logger_error "show keepalived.conf error"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
show()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_debug "checking parameters ..."
  O_TARGET=${O_TARGET:-all}
  logger_debug "O_TARGET=$O_TARGET"

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE
  O_P_PORT=$O_SIDORPORT

  if [[ $O_TARGET != "ha" && $O_TARGET != "keepalived" && $O_TARGET != "all" ]]; then
    logger_error "invalid target: $_t"
    exit $E_I_INVALID_TARGET
  fi
  if [[ $O_TARGET == "ha" || $O_TARGET == "all" ]]; then
    show_ha
  fi
  if [[ $O_TARGET == "keepalived" || $O_TARGET == "all" ]]; then
    if ! check_instance_ha "$O_DBTYPE" 1; then 
      logger_error "haconf invalid"
      exit $E_I_READ_HACONF
    fi
    read_variables_in_haconf
    print_variables
    show_keepalived
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# show_scheduler
show_scheduler()
{
  return;
}

##
# wait vip.
#
# @param $1 vip
# @param $2 timeout
# @param $3 interval
# @return 0 vip is available
# @return 1 vip is not available
##
wait_vip2()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _vip=$1
  declare -ri _timeout=${2:-$O_WAIT_VIP_TIMEOUT}
  declare -ri _interval=${3:-$O_WAIT_VIP_SLEEP_INTERVAL}
  [[ -z $_vip ]] && { logger_error "vip is null"; exit $E_I_INVALID_VARIABLE; }
  [[ -z $_timeout ]] && { logger_error "timeout is null"; exit $E_I_INVALID_VARIABLE; }
  [[ -z $_interval ]] && { logger_error "interval is null"; exit $E_I_INVALID_VARIABLE; }
  logger_info "waiting for vip to be available: $_vip ..."
  declare _start_time=$(date '+%s')
  declare -i _rc1=1
  declare -i _r=0
  declare -i _cc=0
  declare _current_time
  for (( _current_time=_start_time; _current_time-_start_time<_timeout; _current_time=$(date '+%s') ))
  {
    check_vip_local "$_vip"; _rc1=$?
    logger_debug "please wait ... $((_current_time-_start_time))/$_timeout"
    if ((_rc1==0)); then
      _r=1
      break
    fi
    sleep $_interval
  }
  logger_debug "please wait ... $((_current_time-_start_time))/$_timeout"
  if ((_r!=1)); then
    logger_error "vip is not available: "
    _cc=$E_VIP_NOT_EXIST
  else
    logger_info "vip startup compleleted successfully."
    _cc=$E_VIP_EXIST
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_cc
}
wait_vip()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -i _r
  wait_vip2 "$O_VIP" "$O_WAIT_VIP_TIMEOUT" "$O_WAIT_VIP_SLEEP_INTERVAL"
  _r=$?
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
  return $_r
}


##
# execute host cmd
##
initd_keepalived()
{
  declare -i _r=0
  case "$1" in
    "reload")
      logger_info "reloading keepalived ..."
      declare p
      if [ "$O_DRY_RUN" != "Y" ]; then
        if [ -x "$KEEPALIVED_INITD" ]; then
          $KEEPALIVED_INITD reload
        elif [ -x "$PKILL" ]; then
          $PKILL -1 keepalived
        elif [ -x "$PGREP" ]; then
          $PGREP keepalived|while read p; do $KILL -1 $p; done
        elif [ -x "$PS" ]; then
          $PS -ef|$GREP -- "keepalived -D"|$GREP -v grep|$AWK '{print $2}'|\
          while read p; do $KILL -1 $p; done
        fi
      fi
      _r=$?
      ;;
    "start")
      logger_info "starting keepalived ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        if [ -x "$KEEPALIVED_INITD" ]; then
          $KEEPALIVED_INITD start
        fi
      fi
      _r=$?
      ;;
    "stop")
      logger_info "stopping keepalived ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        if [ -x "$KEEPALIVED_INITD" ]; then
          $KEEPALIVED_INITD stop
        fi
      fi
      _r=$?
      ;;
    *)
      logger_error "invalid verb: $1"
      _r=1
      ;;
  esac
  return $_r
}
initd_lvs_real()
{
  declare -i _r=0
  declare -r LVS_REAL_INITD=/etc/init.d/lvs_real
  if [[ ! -x $LVS_REAL_INITD ]]; then
    logger_error "initd lvs real not found: $LVS_REAL_INITD"
    return 1
  fi
  case "$1" in
    "addcfg")
      logger_info "adding lvs real: $2 ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        $LVS_REAL_INITD stop
        $LVS_REAL_INITD addcfg $2
        $LVS_REAL_INITD start
        /sbin/chkconfig --del lvs_real
        /sbin/chkconfig --add lvs_real
      fi
      ;;
    "delcfg")
      logger_info "deleting lvs real ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        $LVS_REAL_INITD stop
        $LVS_REAL_INITD delcfg
        /sbin/chkconfig --del lvs_real
      fi
      ;;
    "start")
      logger_info "starting lvs real: $2 ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        $LVS_REAL_INITD start $2
      fi
      ;;
    "stop")
      logger_info "stopping lvs real: $2 ..."
      if [ "$O_DRY_RUN" != "Y" ]; then
        $LVS_REAL_INITD stop $2
      fi
      ;;
    *)
      logger_error "invalid verb: $@"
      _r=1
      ;;
  esac
  return $_r
}
execute_hostcmd()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _cmd=$O_EXECUTE_HOSTCMD
  logger_debug "hostcmd=$_cmd"
  if [[ $_cmd == "start_keepalived" ]]; then
    initd_keepalived start
  elif [[ $_cmd == "stop_keepalived" ]]; then
    initd_keepalived stop
  elif [[ $_cmd == "reload_keepalived" ]]; then
    initd_keepalived reload
  elif [[ $_cmd =~ ^add_lvs_real=[0-9.]+$ ]]; then
    declare _ip=${_cmd#*=}
    initd_lvs_real addcfg $_ip
  elif [[ $_cmd =~ ^del_lvs_real$ ]]; then
    initd_lvs_real delcfg
  elif [[ $_cmd =~ ^start_lvs_real=[0-9.]+$ ]]; then
    declare _ip=${_cmd#*=}
    initd_lvs_real start $_ip
  elif [[ $_cmd == "start_lvs_real" ]]; then
    initd_lvs_real start
  elif [[ $_cmd =~ ^stop_lvs_real=[0-9.]+$ ]]; then
    declare _ip=${_cmd#*=}
    initd_lvs_real stop $_ip
  elif [[ $_cmd == "stop_lvs_real" ]]; then
    initd_lvs_real stop
  else
    logger_error "hostcmd not supported: $_cmd"
    exit $E_EXECUTE_HOSTCMD_NOT_SUPPORT
    #eval "$@"
  fi
  if [ $? -ne 0 ]; then
    logger_error "execute hostcmd failed: $_r"
    exit $E_EXECUTE_HOSTCMD_FAIL
  else
    logger_debug "execute hostcmd finished"
    return $E_EXECUTE_HOSTCMD_SUCCESS
  fi
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}


##
# dismiss a standby from the cluster
##
delete_standby2_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  # added for multi-instance
  declare -r _sidorport=$O_P_SIDORPORT
  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP

  logger_debug "editing entry ..."
  declare _entry
  if [ -f "$_f" ]; then
    $AWK "BEGIN{OFS=\"${DELIM}\"} {if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\" || \"$_sidorport\"==\"\")) \$6=\"$O_P_DEST_IP\"; print}" $_f >$_tf || \
    { logger_error "editing entry failed"; return $E_CRT_INS_WRITE_NEW; }
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
delete_standby2_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP
  declare -r _tf2=$KEEPALIVED_CONF_TMP2
  declare -i _flag_begin _flag_end

  logger_debug "finding virtual server ..."
  _flag_begin=$($AWK '/^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/{print NR;exit}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_begin=$_flag_begin"
  if [[ $_flag_begin -eq 0 ]]; then
    logger_error "flag begin not found"
    return $E_CRT_INS_WRITE_NEW
  fi
  _flag_end=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
    exit
  }
}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "finding virtual server begin end failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag_end=$_flag_end"
  if [ $_flag_end -le 1 ]; then
    logger_error "flag end not found"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing virtual server part to temp file ..."
  $SED -n "${_flag_begin},${_flag_end}p" $_f >$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing virtual server part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "editing sorry_server in virtual server ..."
  logger_debug "#O_SORRYSERVER_ARRAY=${#O_SORRYSERVER_ARRAY[@]}"
  for((_i=0;_i<${#O_SORRYSERVER_ARRAY[@]};_i++)); do
    logger_debug "editing realserver: ${O_SORRYSERVER_ARRAY[$_i]} ..."
    $AWK '
{
  if ( $0 ~ /^[[:space:]]*sorry_server[[:space:]]+'"${O_SORRYSERVER_ARRAY[$_i]}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*$/ )
  {
    ;
  }
  else
    print
}
' $_tf >$_tf2 && $MV -f $_tf2 $_tf
    if [ $? -ne 0 ]; then
      logger_error "editing entry failed: ${O_SORRYSERVER_ARRAY[$_i]}"
      return $E_CRT_INS_WRITE_NEW
    fi
  done

  logger_debug "combining 3 parts to temp file ..."
  $SED -n "1,$((_flag_begin-1))p" $_f >$_tf2 && \
  $CAT $_tf >>$_tf2 && \
  $SED -n "$((_flag_end+1)),\$p" $_f >>$_tf2
  if [ $? -ne 0 ]; then
    logger_error "combining 3 parts to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf2" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
delete_standby2()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="delete_standby2_${_t}"
    if [[ $_dbtype == "mysql" && $_t == "keepalived" ]]; then
      true
    else
      logger_debug "$_func $_dbtype"
      $_func "$_dbtype" || \
      { logger_error "dismiss standby failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
    fi
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
delete_standby()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_debug "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE

  if [[ -z "$O_SORRYSERVER" ]]; then
    logger_error "O_SORRYSERVER is null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  set_sorryserver_array

  logger_debug "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    logger_error "haconf invalid"
    exit $E_I_READ_HACONF
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  set_default_variables "$O_DBTYPE"

  logger_debug "parsing sorryserver ..."
  declare _new_sorryserver
  declare -i _i _j
  for((_i=0; _i<${#O_SORRYSERVER_ARRAY[@]}; _i++)); do
    declare _rs=${O_SORRYSERVER_ARRAY[$_i]}
    if [[ $_rs == ${O_P_DEST_IP_ARRAY[0]} ]]; then
      logger_error "local ip cannot be dismissed: $_rs"
      exit $E_I_INVALID_VARIABLE
    elif in_array "$_rs" "${O_P_DEST_IP_ARRAY[@]}"; then
      _new_sorryserver="$_new_sorryserver,$_rs"
    else
      logger_debug "sorryserver not exists: $_rs"
    fi
  done
  _new_sorryserver=${_new_sorryserver/#,/}
  if [[ $_new_sorryserver != $O_SORRYSERVER ]]; then
    logger_debug "set new sorryserver"
    O_SORRYSERVER=$_new_sorryserver
    set_sorryserver_array
  fi
  logger_debug "O_SORRYSERVER=$O_SORRYSERVER"

  if [[ -n $O_SORRYSERVER ]]; then
    logger_info "analyzing dest ip ..."
    declare _new_dest_ip
    declare -i _i _j
    for((_i=0; _i<${#O_P_DEST_IP_ARRAY[@]}; _i++)); do
      declare _d=${O_P_DEST_IP_ARRAY[$_i]}
      if in_array "$_d" "${O_SORRYSERVER_ARRAY[@]}"; then
        logger_debug "dest ip exists: $_d"
      else
        _new_dest_ip="$_new_dest_ip,$_d"
      fi
    done
    _new_dest_ip=${_new_dest_ip/#,/}
    if [[ $_new_dest_ip != $O_P_DEST_IP ]]; then
      logger_debug "set new dest ip"
      O_P_DEST_IP=$_new_dest_ip
      set_dest_ip_array
    fi
    logger_debug "O_P_DEST_IP=$O_P_DEST_IP"

    logger_info "checking validation of variables ..."
    check_variables "$O_DBTYPE" || \
    { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
    print_variables

    logger_info "checking existence of vrrp instance ..."
    check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
    { logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

    logger_info "backuping conf ..."
    backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

    logger_info "deleting sorry servers ..."
    delete_standby2 "$O_TARGET" "$O_DBTYPE"
    if [ $? -ne 0 ]; then
      logger_error "deleting sorry servers failed"
      logger_info "removing backup conf ..."
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_CREATE_INSTANCE
    fi

  else
    logger_debug "editing real server not needed"
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# added for multi-instance
delete_standby_scheduler()
{
  logger_info "entering function ${FUNCNAME[0]}()..."
  logger_info "processing sidorport ..."
  
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  for _t in ${O_TARGET//,/ };do
    if [[ $_t != "ha" && $_t != "keepalived" ]];then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  
  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]];then
    logger_error "parameter VRRPINSTANCE and VIP are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  #if [[ -z $O_REALSERVER ]];then
  #  logger_error "the realserver you want to add is null,program will exit..."
  #  exit $E_I_INVALID_VRRPINSTANCE
  #fi
  
  declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE
  declare -r _temp_sorryserver=$O_SORRYSERVER
  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be added to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  #sidorports=${O_SIDORPORT:-$O_P_SIDORPORT} 

  logger_info "processing the realserver add on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."
    
    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    #O_REALSERVER=$_temp_realserver
    O_SORRYSERVER=$_temp_sorryserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port
    
    delete_standby
  done
  
  logger_info "leaving function $FUNCNAME[0]()..." 
}

##
# join a standby to the cluster
##
add_standby2_ha()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -r _vi=$O_P_VRRPINSTANCE
  declare -r _vip=$O_P_VIP
  # added for mylti-instance
  declare -r _sidorport=$O_P_SIDORPORT

  declare -r _f=$HACONF
  declare -r _tf=$HACONF_TMP

  logger_debug "editing entry ..."
  declare _entry
  if [ -f "$_f" ]; then
    $AWK "BEGIN{OFS=\"${DELIM}\"} {if( (\$8==\"$_vi\"||\"$_vi\"==\"\") && (\$9==\"$_vip\"||\"$_vip\"==\"\") && (\$2==\"$_dbtype\"||\"$_dbtype\"==\"\") && (\$4==\"$_sidorport\"||\"$_sidorport\"==\"\")) \$6=\"$O_P_DEST_IP\"; print}" $_f >$_tf || \
    { logger_error "editing entry failed"; return $E_CRT_INS_WRITE_NEW; }
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
add_standby2_keepalived()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _dbtype=$1
  declare -i _r
  declare -i _i
  declare -r _f=$KEEPALIVED_CONF
  declare -r _tf=$KEEPALIVED_CONF_TMP
  declare -r _tf2=$KEEPALIVED_CONF_TMP2
  declare -i _flag

  logger_debug "finding virtual server ..."
  _flag=$($AWK '
{
  if ( ($0 ~ /^[[:space:]]*virtual_server[[:space:]]+'"${O_P_VIP}"'[[:space:]]+'"${O_P_SIDORPORT}"'[[:space:]]*{[[:space:]]*$/) )
  {
    I=1
    while (I>0) {
      getline
      if ($0 ~ /}/) I--
      if ($0 ~ /{/) I++
    } #end of while I
    print NR
  }
}' $_f)
  if [ $? -ne 0 ]; then
    logger_error "appending new entry to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi
  logger_debug "_flag=$_flag"
  if [ $_flag -le 1 ]; then
    logger_error "flag not found"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing part1 to temp file ..."
  $SED -n "1,$((_flag-1))p" $_f >$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing 1st part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "writing real server to temp file ..."
  logger_debug "#O_SORRYSERVER_ARRAY=${#O_SORRYSERVER_ARRAY[@]}"
  for((_i=0;_i<${#O_SORRYSERVER_ARRAY[@]};_i++)); do
    logger_debug "adding sorryserver: ${O_SORRYSERVER_ARRAY[$_i]} ..."
    cat >>$_tf <<EOF
    sorry_server ${O_SORRYSERVER_ARRAY[$_i]} ${O_P_SIDORPORT}
EOF
    if [ $? -ne 0 ]; then
      logger_error "appending new entry to temp file failed"
      return $E_CRT_INS_WRITE_NEW
    fi
  done

  logger_debug "writing part2 to temp file ..."
  $SED -n "$_flag,\$p" $_f >>$_tf
  if [ $? -ne 0 ]; then
    logger_error "writing 2nd part to temp file failed"
    return $E_CRT_INS_WRITE_NEW
  fi

  logger_debug "overwriting original file with the new temp file ..."
  file_overwrite "$_tf" "$_f" || \
  { logger_error "overwriting original file with the new temp file failed"; return $E_CRT_INS_OVERWRITE_NEW; }

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
add_standby2()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."
  declare -r _target=$1
  declare -r _dbtype=$2
  declare _t
  for _t in ${_target//,/ }; do
    declare _func="add_standby2_${_t}"
    if [[ $_dbtype == "mysql" && $_t == "keepalived" ]]; then
      true
    else
      logger_debug "$_func $_dbtype"
      $_func "$_dbtype" || \
      { logger_error "dismiss standby failed $_t $_dbtype"; return $E_CRT_INS_CREATE_INSTANCE; }
    fi
  done
  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}
add_standby()
{
  logger_debug "entering function ${FUNCNAME[0]}() ..."

  logger_debug "checking parameters ..."
  logger_debug "O_TARGET=$O_TARGET"
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_debug "O_BACKUP_CONF=$O_BACKUP_CONF"
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_debug "O_TARGET=$O_TARGET"
  declare _t
  for _t in ${O_TARGET//,/ }; do
    if [[ $_t != "ha" && $_t != "keepalived" ]]; then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done

  if [[ -z $O_VRRPINSTANCE && -z $O_VIP && -z $O_DBTYPE ]]; then
    logger_error "parameter VRRPINSTANCE and VIP and DBTYPE are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  O_P_VRRPINSTANCE=$O_VRRPINSTANCE
  O_P_VIP=$O_VIP
  O_P_DBTYPE=$O_DBTYPE

  if [[ -z "$O_SORRYSERVER" ]]; then
    logger_error "O_SORRYSERVER is null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi
  set_sorryserver_array

  logger_debug "reading haconf ..."
  if ! check_instance_ha "$O_DBTYPE" 1; then 
    logger_error "haconf invalid"
    exit $E_I_READ_HACONF
  fi
  read_variables_in_haconf

  O_DBTYPE=${O_DBTYPE:-$O_P_DBTYPE}
  logger_debug "O_DBTYPE=$O_DBTYPE"
  if [[ $O_DBTYPE != "mysql" && $O_DBTYPE != "lvs" && $O_DBTYPE != "oracle" && $O_DBTYPE != "pxcw" && $O_DBTYPE != "pxcr" && $O_DBTYPE != "mongodb" ]]; then
    logger_error "invalid dbtype: $O_DBTYPE"
    exit $E_I_INVALID_DBTYPE
  fi
  set_default_variables "$O_DBTYPE"

  logger_debug "parsing sorryserver ..."
  declare _new_sorryserver
  declare -i _i _j
  for((_i=0; _i<${#O_SORRYSERVER_ARRAY[@]}; _i++)); do
    declare _d=${O_SORRYSERVER_ARRAY[$_i]}
    if [[ $_d == ${O_P_DEST_IP_ARRAY[0]} ]]; then
      logger_error "local ip cannot be joined as standby: $_d"
      exit $E_I_INVALID_VARIABLE
    elif in_array "$_d" "${O_P_DEST_IP_ARRAY[@]}"; then
      logger_debug "sorryserver exists: $_d"
    else
      _new_sorryserver="$_new_sorryserver,$_d"
    fi
  done
  _new_sorryserver=${_new_sorryserver/#,/}
  if [[ $_new_sorryserver != $O_SORRYSERVER ]]; then
    logger_debug "set new sorryserver"
    O_SORRYSERVER=$_new_sorryserver
    set_sorryserver_array
  fi
  logger_debug "O_SORRYSERVER=$O_SORRYSERVER"

  if [[ -n $O_SORRYSERVER ]]; then
    logger_info "analyzing dest ip ..."
    O_P_DEST_IP="${O_P_DEST_IP},$O_SORRYSERVER"
    O_P_DEST_IP=${O_P_DEST_IP/#,/}
    set_dest_ip_array
    logger_debug "O_P_DEST_IP=$O_P_DEST_IP"

    logger_info "checking validation of variables ..."
    check_variables "$O_DBTYPE" || \
    { logger_error "checking validation of variables failed"; print_variables "INFO"; exit $E_I_INVALID_VARIABLE; }
    print_variables

    logger_info "checking existence of vrrp instance ..."
    check_instance "$O_TARGET" "$O_DBTYPE" 1 || \
    { logger_error "vrrp instance not exists"; exit $E_I_NO_INSTANCE; }

    logger_info "backuping conf ..."
    backup_conf "$O_BACKUP_CONF" "$O_BACKUP_TS"

    logger_info "deleting sorry servers ..."
    add_standby2 "$O_TARGET" "$O_DBTYPE"
    if [ $? -ne 0 ]; then
      logger_error "deleting sorry servers failed"
      logger_info "removing backup conf ..."
      restore_conf "$O_BACKUP_CONF" "$O_BACKUP_TS" "none" "" "N"
      exit $E_I_CREATE_INSTANCE
    fi

  else
    logger_debug "editing real server not needed"
  fi

  logger_debug "leaving function ${FUNCNAME[0]}() ..."
}

# added for multi-instance purpose
add_standby_scheduler()
{
  logger_info "entering function ${FUNCNAME[0]}()..."
  logger_info "processing sidorport ..."
  
  O_TARGET=${O_TARGET:-all}
  O_BACKUP_CONF=${O_BACKUP_CONF:-$O_TARGET}
  logger_info "O_BACKUP_CONF = $O_BACKUP_CONF"
  
  O_TARGET=${O_TARGET//all/ha,keepalived}
  logger_info "O_TARGET = $O_TARGET"

  for _t in ${O_TARGET//,/ };do
    if [[ $_t != "ha" && $_t != "keepalived" ]];then
      logger_error "invalid target: $_t"
      exit $E_I_INVALID_TARGET
    fi
  done
  
  if [[ -z $O_VRRPINSTANCE && -z $O_VIP ]];then
    logger_error "parameter VRRPINSTANCE and VIP are null"
    exit $E_I_INVALID_VRRPINSTANCE
  fi

  #if [[ -z $O_REALSERVER ]];then
  #  logger_error "the realserver you want to add is null,program will exit..."
  #  exit $E_I_INVALID_VRRPINSTANCE
  #fi
  
  declare -r _temp_realserver=$O_REALSERVER
  declare -r _temp_vip=$O_VIP
  declare -r _temp_vi=$O_VRRPINSTANCE
  declare -r _temp_sorryserver=$O_SORRYSERVER
  declare _sidorports
  #declare _t
  #declare _sidorports_keepalived

  if [[ -z $O_SIDORPORT && -z $O_P_SIDORPORT ]];then
    logger_warn "port is not assigned, realserver will be added to all the instance running on the vip..."
    if [ -f $HACONF ];then
      _sidorports=$($AWK "BEGIN{OFS=\"${DELIM}\";_t=\"\"}{ if ((\$8==\"${O_VRRPINSTANCE}\" || \"${O_VRRPINSTANCE}\"==\"\") && (\$9==\"${O_VIP}\" || \"${O_VIP}\"==\"\")) _t=(\$4 \",\" _t)}END{print _t}" $HACONF)
    fi
  else
    _sidorports=${O_SIDORPORT:-$O_P_SIDORPORT}
  fi
  #sidorports=${O_SIDORPORT:-$O_P_SIDORPORT} 

  logger_info "processing the realserver add on VIP:$_temp_vip,VINSTANCE:$_temp_vi..."
  for _port in ${_sidorports//,/ };do
    logger_info "processiong instance on port:$_port...."
    
    O_VRRPINSTANCE=$_temp_vi
    O_VIP=$_temp_vip
    #O_REALSERVER=$_temp_realserver
    O_SORRYSERVER=$_temp_sorryserver

    O_SIDORPORT=$_port
    O_P_SIDORPORT=$_port
    
    add_standby
  done
  
  logger_info "leaving function $FUNCNAME[0]()..."
}
##
# upgrade a realserver to a standby
##
upgrade_realserver()
{
  return
}


E_SUCCESS=0
E_M_NO_PERMISSION=8
E_M_NO_PATH=9
E_VIP_EXIST=0
E_VIP_NOT_EXIST=10
E_VIP_KA_RELOAD_ERROR=11
E_CHK_VAR_INVALID_VARIABLE=20
E_CHK_INS_WRONG_NUMBER=21
E_CHK_INS_COUNT_ERROR=22
E_CHK_INS_CHECK_INSTANCE=23
E_CRT_INS_WRITE_OLD=24
E_CRT_INS_WRITE_NEW=25
E_CRT_INS_OVERWRITE_NEW=26
E_CRT_INS_CREATE_INSTANCE=27
E_I_INVALID_TARGET=30
E_I_INVALID_DBTYPE=31
E_I_INVALID_VARIABLE=32
E_I_NO_INSTANCE=33
E_I_CREATE_INSTANCE=34
E_I_INVALID_VRRPINSTANCE=35
E_I_READ_HACONF=36
E_EXECUTE_HOSTCMD_SUCCESS=0
E_EXECUTE_HOSTCMD_FAIL=41
E_EXECUTE_HOSTCMD_NOT_SUPPORT=42

for ((i=1;i<=$#;i++)); do
  [ "${!i}" = "--eval-params" ] && EVAL_PARAMS=Y
  if [[ "${!i}" = "--no-apex-result" ]]; then
    O_NO_APEX_RESULT="Y"
  else
    O_NO_APEX_RESULT="N"
  fi
done
[ "$EVAL_PARAMS" = "Y" ] && eval set -- "$@"

apex_onexit() {
  local exit_status=${1:-$?}
  local -r s="APEX_RESULT=$exit_status,ha.sh,Exiting $0 with $exit_status, please check log for detail." 
  if [ "$LOG4SH_READY" = "Y" ]; then
    if [[ $exit_status -ne 0 ]]; then
      logger_error "$s"
      print_call_stack_trace
    else
      logger_info "$s"
    fi
  fi
  echo "$s"
  #exit $exit_status
}
if [[ $O_NO_APEX_RESULT != "Y" ]]; then
  trap apex_onexit EXIT INT TERM
fi

# setting up PATH enviroment if needed
ls >/dev/null 2>&1 || export PATH=/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
ls >/dev/null 2>&1 || { echo "$(data '+%Y-%m-%d %H:%M:%S') ERROR [$$] Please set PATH env." >&2; exit $E_M_NO_PATH; }

#main
DIRNAME=$(getdirname)
PROGNAME=$(getbasename)
APPNAME=$(getappname2)
APPFILENAME=$PROGNAME[$$]
CURR_DATE=$(date '+%Y%m%d')
declare -i FILESYSTEM_AVAILABLE_SIZE=50000 #KB
declare -i fssize
fssize=$(df -P /tmp| tail -n1 |awk '{print $4}')
if ((fssize<FILESYSTEM_AVAILABLE_SIZE)); then
  echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR $APPFILENAME /tmp insufficient" >&2
  exit $E_M_NO_PERMISSION
fi
fssize=$(df -P /var/log| tail -n1 |awk '{print $4}')
if ((fssize<FILESYSTEM_AVAILABLE_SIZE)); then
  echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR $APPFILENAME /var/log insufficient" >&2
  exit $E_M_NO_PERMISSION
fi
LOG_DIR=/var/log/keepalived
[ ! -d "$LOG_DIR" ] && mkdir -p $LOG_DIR/
[ ! -x "$LOG_DIR" -o ! -w "$LOG_DIR" ] && { echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR $APPFILENAME $LOG_DIR is inaccessiable: Permission denied" >&2; exit $E_M_NO_PERMISSION; }
LOG_NAME=$LOG_DIR/${APPNAME}_${CURR_DATE}.log
TMPDIR=/tmp/keepalived.$$.$RANDOM
[ ! -d "$TMPDIR" ] && mkdir -p $TMPDIR
[ ! -x "$TMPDIR" -o ! -w "$TMPDIR" ] && { echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR $APPFILENAME $TMPDIR is inaccessiable: Permission denied" >&2; exit $E_M_NO_PERMISSION; }
HACONF_TMP=$TMPDIR/haconf.tmp
HACONF_TMP2=$TMPDIR/haconf.tmp2
KEEPALIVED_CONF_TMP=$TMPDIR/keepalived.conf.tmp
KEEPALIVED_CONF_TMP2=$TMPDIR/keepalived.conf.tmp2
TEMPFILE=$TMPDIR/tempfile.tmp
TEMPFILE2=$TMPDIR/tempfile2.tmp
IP_TMP=$TMPDIR/ip.tmp

trap - EXIT INT TERM
LOG4SH_SOUCRE=$DIRNAME/log4sh
[ ! -r $LOG4SH_SOUCRE ] && LOG4SH_SOUCRE=/usr/local/bin/log4sh
[ ! -r $LOG4SH_SOUCRE ] && LOG4SH_SOUCRE=/usr/bin/log4sh
[ ! -r $LOG4SH_SOUCRE ] && LOG4SH_SOUCRE=/bin/log4sh
LOG4SH_PROPERTIES=$DIRNAME/$APPNAME.log4sh.properties
LOG4SH_DEFAULT_LAYOUT="%d %p %F %m%n"
LOG4SH_DEFAULT_LOGNAME="$LOG_DIR/${APPNAME}_$(date '+%Y%m%d').log"
if [ ! -r $LOG4SH_SOUCRE ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR $APPFILENAME loading log4sh failed." >&2
  exit $E_M_NO_PATH
fi
if [ -r $LOG4SH_PROPERTIES ]; then
  LOG4SH_CONFIGURATION=$LOG4SH_PROPERTIES source $LOG4SH_SOUCRE
  logger_setFilename $APPFILENAME
  #reset filename
  appender_file_setFile FILE "$LOG4SH_DEFAULT_LOGNAME"
  appender_activateOptions FILE
else
  #echo "$(date '+%Y-%m-%d %H:%M:%S') WARN $APPFILENAME properites not found: $LOG4SH_PROPERTIES"
  LOG4SH_CONFIGURATION='none' source $LOG4SH_SOUCRE
  log4sh_resetConfiguration
  logger_setLevel INFO
  logger_setFilename $APPFILENAME
  logger_addAppender STDOUT
  appender_setType STDOUT ConsoleAppender
  appender_setPattern STDOUT "$LOG4SH_DEFAULT_LAYOUT"
  appender_setLayout STDOUT PatternLayout
  appender_activateOptions STDOUT
  logger_addAppender FILE
  appender_setType FILE FileAppender
  appender_file_setFile FILE "$LOG4SH_DEFAULT_LOGNAME"
  appender_setPattern FILE "$LOG4SH_DEFAULT_LAYOUT"
  appender_setLayout FILE PatternLayout
  appender_activateOptions FILE
fi
LOG4SH_READY=Y

declare -a on_exit_items
add_on_exit()
{
  declare -r _in="$*"
  declare -i _i
  for ((_i=0;_i<${#on_exit_items[@]};_i++)); do
    if [[ "${on_exit_items[$_i]}" = "$_in" ]]; then
      logger_debug "add_on_exit:old:$_i:$_in"
      return $_i
    fi
  done
  logger_debug "add_on_exit:new:$_i:$_in"
  on_exit_items[$_i]="$_in"
  return $_i
}

SAVED_TRAP_FILE=$TMPDIR/saved_trap
trap > $SAVED_TRAP_FILE
cleanup()
{
  declare _r=$?
  if [[ $O_NO_APEX_RESULT != "Y" ]]; then
    apex_onexit $_r
  fi

  rmfile() { declare _f; for _f; do if [ -f "$_f" ]; then logger_debug "removing $_f ..."; $RM -f $_f; fi; done; }
  declare f
  for f in "${on_exit_items[@]}"; do
      rmfile "$f"
  done

  declare saved_trap
  declare _sig=$1
  case "$_sig" in
    EXIT|SIGINT|SIGTERM)
      saved_trap=$(sed -n "s/^trap -- '\(.*\)' ${_sig}$/\1/p" $SAVED_TRAP_FILE)
      #logger_debug "saved_trap=$saved_trap"
      $saved_trap
      ;;
  esac
  \rm -f $SAVED_TRAP_FILE

  if [ -d "$TMPDIR" ]; then
    if ! [ "$(ls -A $TMPDIR)" ]; then
      \rmdir $TMPDIR
    else
      if [ "$O_FLAG_DEBUG" != "Y" ]; then
        [ -d "$TMPDIR" ] && \rm -rf $TMPDIR
      fi
    fi
  fi
}
trap 'cleanup EXIT' EXIT
trap 'cleanup SIGINT' SIGINT
trap 'cleanup SIGTERM' SIGTERM


O_WAIT_VIP_TIMEOUT_DEF=120
O_WAIT_VIP_TIMEOUT=$O_WAIT_VIP_TIMEOUT_DEF
O_WAIT_VIP_SLEEP_INTERVAL_DEF=3
O_WAIT_VIP_SLEEP_INTERVAL=$O_WAIT_VIP_SLEEP_INTERVAL_DEF

clear_variables

# initialize runtime options
usage()
{
  $CAT <<EOF
Usage: $PROGNAME [OPTION] ... {OPERATION} -- [PARAMETERS] ... 
EOF
}

version()
{
$CAT <<EOF
$PROGNAME $VERSION
EOF
}
SHORT_OPTS="hfing:vqdTDIVP"
LONG_OPTS="help version force interactive dry-run
log-level: verbose quiet debug
cleanup-temp:: cleanup-temp-retention: cleanup-log:: cleanup-log-retention:
no-apex-result
reload-keepalived:: preserve-databases::
haconf: c1: keepalived-conf: c2:
haconf-bak: b1: keepalived-conf-bak: b2:
backup-conf:: restore-conf::
backup-ts: restore-ts: restore-preserve-backup::
add target: dbtype:
delete vrrpinstance: vip: sidorport:
edit-ha
add-realserver: delete-realserver: edit-realserver:
delete-standby: add-standby:
create interface:
recreate show
wait-vip wait-vip-timeout: wait-vip-sleep-interval:
execute-hostcmd:
"
[ $# -gt 0 ] && ARGS=$(getopt -n$PROGNAME -o "$SHORT_OPTS" -l "$LONG_OPTS" -- "$@") || { usage; exit 1; }
eval set -- "$ARGS"
while [ $# -gt 0 ]; do
  case "$1" in
    #basic options
    -h|--help) usage; exit ;;
    -f|--force) O_FLAG_FORCE=Y ;;
    -i|--interactive) O_FLAG_INTERACTIVE=Y ;;
    -n|--dry-run) O_DRY_RUN=Y ;;
    -g|--log-level) logger_setLevel $2; shift ;;
    -v|--verbose)
       logger_setLevel INFO
       ;;
    -q|--quiet) logger_setLevel ERROR ;;
    -d|--debug) O_FLAG_DEBUG=Y; logger_setLevel DEBUG ;;
    --version) version; exit;;
    --cleanup-temp) O_CLEANUP_TEMP=$(toupper "${2:-Y}"); shift ;;
    --cleanup-log) O_CLEANUP_LOG=$(toupper "${2:-Y}"); shift ;;
    --cleanup-temp-retention) O_CLEANUP_TEMP_RETENTION=$2; shift ;;
    --cleanup-log-retention) O_CLEANUP_LOG_RETENTION=$2; shift ;;

    --no-apex-result) O_NO_APEX_RESULT="Y" ;;

    #additional options
    --reload-keepalived) O_RELOAD_KEEPALIVED=$(toupper "${2:-Y}"); shift ;;
    --preserve-databases) O_PRESERVE_DATABASES=$(toupper "${2:-Y}"); shift ;;
    --c1|--haconf) O_HACONF=$2; shift ;;
    --c2|--keepalived-conf) O_KEEPALIVED_CONF=$2; shift ;;
    --b1|--haconf-bak) O_HACONF_BAK=$2; shift ;;
    --b2|--keepalived-conf-bak) O_KEEPALIVED_CONF_BAK=$2; shift ;;
    --backup-ts) O_BACKUP_TS=$2; shift ;;
    --restore-ts) O_RESTORE_TS=$2; shift ;;
    --restore-preserve-backup) O_RESTORE_PRESERVE_BACKUP=$(toupper "${2:-Y}"); shift ;;

    #verbs
    --add) O_OPERATION="add_instance" ;;
    --delete) O_OPERATION="delete_instance" ;;

    --target|-T) O_TARGET=$(tolower "$2"); shift ;;
    --dbtype|-D) O_DBTYPE=$(tolower "$2"); shift ;;
    --vrrpinstance|-I) O_VRRPINSTANCE=$(tolower "$2"); shift ;;
    --vip|-V) O_VIP=$2; shift ;;
    --sidorport|-P) O_SIDORPORT=$2; shift ;;
    --interface) O_VIP_INTERFACE=$2; shift ;;

    --edit-ha) O_OPERATION="edit_ha" ;;
    --add-realserver) O_OPERATION="add_realserver"; O_REALSERVER=$2; shift ;;
    --delete-realserver) O_OPERATION="delete_realserver"; O_REALSERVER=$2; shift ;;
    --edit-realserver) O_OPERATION="edit_realserver"; O_REALSERVER=$2; shift ;;

    --delete-standby) O_OPERATION="delete_standby"; O_SORRYSERVER=$2; shift ;;
    --add-standby) O_OPERATION="add_standby"; O_SORRYSERVER=$2; shift ;;

    --create) O_OPERATION="create_keepalived_conf" ;;
    --recreate) O_OPERATION="recreate_keepalived_conf" ;;
    --show) O_OPERATION="show" ;;

    --backup-conf)
      [ "$O_OPERATION" = "" ] && O_OPERATION="backup_conf"
      O_BACKUP_CONF=$(tolower "${2:-all}"); shift ;;
    --restore-conf) O_OPERATION="restore_conf"; O_RESTORE_CONF=$(tolower "${2:-all}"); shift ;;

    --wait-vip) O_OPERATION="wait_vip" ;;
    --wait-vip-timeout) O_WAIT_VIP_TIMEOUT=$2; shift ;;
    --wait-vip-sleep-interval) O_WAIT_VIP_SLEEP_INTERVAL=$2; shift ;;

    --execute-hostcmd) O_OPERATION="execute_hostcmd"; O_EXECUTE_HOSTCMD=$2; shift  ;;


   #evaluate parameters
    --) shift
      declare e
      for e; do
        eval_param "$e"
      done
      break ;;
    #bad options
    -*) usage; exit 1 ;;
    *) usage; exit 1 ;;
  esac
  shift
done

if [ $(id -un) != "root" ]; then
  logger_error "This script must to be run as super user."
  exit 1
fi

#echo "O_DBTYPE $O_DBTYPE"
[ "$O_FLAG_FORCE" = "Y" ] && O_FLAG_INTERACTIVE=N
[ "$O_RELOAD_KEEPALIVED" != "Y" ] && O_RELOAD_KEEPALIVED=N
[ "$O_PRESERVE_DATABASES" != "Y" ] && O_PRESERVE_DATABASES=N
[ -z "$O_BACKUP_CONF" ] && O_BACKUP_CONF="all"
[ -z "$O_RESTORE_CONF" ] && O_RESTORE_CONF="all"
[ -n "$O_HACONF" ] && HACONF=$O_HACONF
[ -n "$O_KEEPALIVED_CONF" ] && KEEPALIVED_CONF=$O_KEEPALIVED_CONF
logger_debug "HACONF=$HACONF"
logger_debug "KEEPALIVED_CONF=$KEEPALIVED_CONF"
[ -n "$O_HACONF_BAK" ] && HACONF_BAK=$O_HACONF_BAK
[ -n "$O_KEEPALIVED_CONF_BAK" ] && KEEPALIVED_CONF_BAK=$O_KEEPALIVED_CONF_BAK
declare _ts=$(date '+%Y%m%d%H%M%S')
[ -z "$O_BACKUP_TS" ] && O_BACKUP_TS=$_ts
[ -z "$O_RESTORE_TS" ] && O_RESTORE_TS=$_ts
DELIM=$'\t'
[[ -z $O_CLEANUP_TEMP ]] && O_CLEANUP_TEMP=Y
[[ -z $O_CLEANUP_LOG ]] && O_CLEANUP_LOG=Y
[[ -z $O_CLEANUP_TEMP_RETETION ]] && O_CLEANUP_TEMP_RETETION=7
[[ -z $O_CLEANUP_LOG_RETETION ]] && O_CLEANUP_LOG_RETETION=500

#logger_debug "DIRNAME=$DIRNAME"
#logger_debug "PROGNAME=$PROGNAME"
#logger_debug "APPNAME=$APPNAME"
#logger_debug "LOG_DIR=$LOG_DIR"
#logger_debug "LOG_NAME=$LOG_NAME"
#logger_debug "TMPDIR=$TMPDIR"
#logger_debug "HACONF_TMP=$HACONF_TMP"
#logger_debug "KEEPALIVED_CONF_TMP=$KEEPALIVED_CONF_TMP"
#logger_debug "TEMPFILE=$TEMPFILE"
#logger_debug "TEMPFILE2=$TEMPFILE2"

add_on_exit "$HACONF_TMP"
add_on_exit "$HACONF_TMP2"
add_on_exit "$KEEPALIVED_CONF_TMP"
add_on_exit "$KEEPALIVED_CONF_TMP2"
add_on_exit "$TEMPFILE"
add_on_exit "$TEMPFILE2"
add_on_exit "$IP_TMP"

PHYSICAL_INTERFACE=$(os_get_interface)
PHYSICAL_IP=$(os_get_hostip)
logger_debug "PHYSICAL_INTERFACE=$PHYSICAL_INTERFACE"
logger_debug "PHYSICAL_IP=$PHYSICAL_IP"
#set_default_variables

declare _s
if [[ $O_CLEANUP_TEMP == "Y" ]]; then
  logger_debug "cleaning up tempfiles ..."
  find /tmp -maxdepth 1 -type d -name "keepalived.*.*" -mtime +${O_CLEANUP_TEMP_RETETION} -exec /bin/rm -rf {} \; -ls | \
  while read _s; do logger_info "deleting $_s ..."; done
fi
if [[ $O_CLEANUP_LOG == "Y" ]]; then
  logger_debug "cleaning up logfiles ..."
  find /var/log/keepalived -type f -mtime +${O_CLEANUP_LOG_RETETION} -exec /bin/rm -rf {} \; -ls | \
  while read _s; do logger_info "deleting $_s ..."; done
fi
unset _s

declare -i rc=0
case "$O_OPERATION" in
  "add_instance") add_instance ;;
  "delete_instance") delete_instance ;;
  "edit_ha") edit_ha_scheduler ;;
  "add_realserver") add_realserver_scheduler ;;
  "delete_realserver") delete_realserver_scheduler ;;
  "edit_realserver") edit_realserver_scheduler ;;
  "backup_conf") backup_conf ;;
  "restore_conf") restore_conf ;;
  "create_keepalived_conf") create_keepalived_conf ;;
  "recreate_keepalived_conf") create_keepalived_conf "recreate" ;;
  "show") show ;;
  "wait_vip") wait_vip ;;
  "execute_hostcmd") execute_hostcmd ;;
  "delete_standby") delete_standby_scheduler ;;
  "add_standby") add_standby_scheduler ;;
  *) logger_info "Nothing to do." ;;
esac
rc=$?
if [ $? -eq 0 ]; then
  if [ "$O_RELOAD_KEEPALIVED" = "Y" ]; then
    logger_info "reloading keepalived ..."
    initd_keepalived reload || rc=$E_VIP_KA_RELOAD_ERROR
  fi
fi

if [[ $rc -ne 0 ]]; then
  logger_error "Program terminated with errors."
else
  logger_debug "Program finished successfully."
fi

exit $rc

:<<'MULTILINE_COMMENTS'
line1
line2
...
MULTILINE_COMMENTS

EOF
