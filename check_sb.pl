#!/usr/bin/env perl
# Description：检查keepalived脑裂
# 退出状态码说明：   异常: exit 1 正常: exit 0
# Authors:  
#   zhaoyunbo

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/common";
use lib "$Bin/etc";
use Myconfig;
use ManagerUtil;
use CheckWvip;

# 接收参数
my ( $g_vrrpins );
GetOptions(
    "vrrp-instance=s" => \$g_vrrpins
);
# 检查参数值
if ( ! defined($g_vrrpins) ) {
    print "--vrrp-instance= is needed\n";
    exit 0;
}

# 初始化log4perl
my $checkLog = "/var/log/check_sb_$g_vrrpins.log";
my $logConf = q(
    log4perl.rootLogger                = INFO, logfile
    log4perl.appender.logfile          = Log::Log4perl::Appender::File
    log4perl.appender.logfile.mode     = append
    log4perl.appender.logfile.layout   = Log::Log4perl::Layout::PatternLayout
    log4perl.appender.logfile.layout.ConversionPattern = %d{yyyy-MM-dd HH:mm:ss}  [%p] [%M] %m%n
);
$logConf .= "log4perl.appender.logfile.filename = $checkLog";
Log::Log4perl->init( \$logConf );

my $log = Log::Log4perl->get_logger("");  # get logObject

# 标识一次检查流程 
my $flag = int(rand(100000));
$log->info("=  begin($flag) =");

# 设置文件锁，防止脚本并行执行
my $lockFile="/tmp/check_sb_$g_vrrpins.lock";
$log->debug("start get lockfile");
my $flockStatus=getFileLock("$lockFile");
my $exitCode = $$flockStatus{'exitCode'};
if ($exitCode != 0){
    $log->error("check_sb.pl is running");
    $log->info("=  end($flag) =");
    exit 0;
}
$log->debug("get lockfile: $lockFile success");

# 创建Myconfig对象
my $myconfObj = new Myconfig(); 
if ( !$myconfObj ) { 
    $myconfObj = new Myconfig(); 
}

# 检查Myconfig.pm自动切换开启情况
$log->debug( "start checking switchover" ); 
my $switchover = $myconfObj->get('switchover');
if ( !$switchover ) {
    $log->error("switchover is not set, do nothing");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");
    exit 0;
} elsif ( $switchover && lc($switchover) ne "on" ) {
    $log->error("switchover is not 'on', do nothing");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");
    exit 0;
}

# 获取高可用全局配置文件
my $haconf = $myconfObj->get('haconfFile');
if ( !$haconf ) { 
    $log->error("get db haconfFile failed.");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}

# 检查高可用全局配置文件能否读
if (! open my $FH, "<", $haconf ){
    $log->error("Cannot open ha config file $haconf");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
} else {
    close( $FH );
}

# 解析高可用全局配置文件
my $haconf_data = `grep -v "#" $haconf | grep -w $g_vrrpins |head -1`;
chomp($haconf_data);
$haconf_data =~ s/^\s+//g;
my ($is_maintained,$dbtype,$dbhome,$dbPort,$dggroup,$hagrp,$gotofault,$vrrpins,$vip,$realgrp) = (split(/\s+/, $haconf_data));


# 检查高可用全局配置文件参数设置

# 若vrrp实例不匹配，则正常退出
if ( lc($g_vrrpins) ne lc($vrrpins) ) {
    $log->info("$g_vrrpins is not $vrrpins");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}
# 若高可用类型不为mysql，则正常退出
if ( lc($dbtype) ne "mysql" ) {
    $log->info("$dbtype is not mysql");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}
# 高可用维护模式为"Y"，正常退出
if ( lc($is_maintained) eq "y" ) {
    $log->info("$vrrpins is in maintain mode");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");
    exit 0;
}

# 若节点不存在写vip，则正常退出
my $isWriteVip = 0;
my $vipNum = `/sbin/ip -4 -o a s | grep -w "$vip" | wc -l`;
chomp($vipNum);
if ( $vipNum == 0 ){
    $log->debug("$vip not run on current node");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}

# 获取当前节点ip地址
my $localIp = getIpAddr();
if ( !$localIp ) {
    $log->error("get local host ip failed");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}
$log->debug("get \$localIp = $localIp");

# 获取高可用组另外一个节点的ip地址
$log->debug("start getting remoteIp"); 	
if ( !$hagrp ) {
    $log->error("get \$hagrp failed, haconf file format error");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}
my ( $oneIp, $theOtherIp ) = ( split( /,/, $hagrp ) );
if ( ($localIp ne $oneIp) && ($localIp ne $theOtherIp) ) {
    $log->error("get wrong \$localIp, it is not in the same HA group");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}
my $remoteIp;
if ( $oneIp eq $localIp ) { 
    $remoteIp = $theOtherIp; 
} else { 
    $remoteIp = $oneIp; 
}
if ( !$remoteIp ) {
    $log->error("get \$remoteIp failed");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
} 
$log->debug("get \$remoteIp = $remoteIp");


my $result = compareIp($localIp, $remoteIp);
if ( $result == 0 ){
    $log->debug("$localIp ip size less than $remoteIp");
    cleanFileLock($lockFile);
    $log->info("=  end($flag) =");
    exit 0;
}

# 检查集群网络隔离情况
# 若节点同时满足以下条件，则该节点keepalived进入fault状态，无法抢占写vip
# (1)节点存在写vip
# (2)节点写vip ping不通网关
# (3)节点ping不通对端keepalived节点
# (4)节点ip大于对端keepalived节点的ip (vrrp优先级相等,ip大的选举为master节点. 此处是避免脑裂后,ip大的回抢vip)

$log->info();
$log->info("$g_vrrpins checking wvip available");

# 单次检查超时检查，默认为9秒
my $g_timeout = $myconfObj->get('checkSbTimeout');
if ( !$g_timeout ) { 
    $log->error("get checkSbTimeout failed.");
    $g_timeout = 9;
}

# 检查失败重试次数,默认为3次
my $retry_times = $myconfObj->get('checkSbRetryTimes');
if ( !$retry_times ) { 
    $log->error("get checkSbRetryTimes failed.");
    $retry_times = 3;
}

my $failed = 0;

for ( my $i=0; $i<$retry_times; $i++ ) {

    my $timeout_msg = "CHK_SB_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeout_msg; }, $sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );
    
    eval {
        # 设置检查超时
        alarm $g_timeout;
        
        my $checkWriteVip = checkWvip($g_vrrpins);
        if (defined($checkWriteVip) && $checkWriteVip == 1){          
            # 节点存在写vip, 写vip ping通网关
            
            $log->info("$g_vrrpins wvip is ok");
            last;
        }else{
            # 节点存在写vip, 写vip ping不通网关
            
            # 尝试ping 2次对方keepalived节点，每次ping超时时间为2秒
            my $cmd = "ping -n -q -W 2 -c 2 '$remoteIp' >/dev/null 2>&1";
            my $exit_code = system("$cmd");
            if ( $exit_code != 0 ){
                # 节点ping不通对端keepalived节点
               
                # 最后一次检查，还存在问题，则: 
                if ( $i == ($retry_times-1) ){
                    
                    my $wvip = getWvip($g_vrrpins); 
                    if ( defined($wvip) && $wvip ) {
                        # 修改haconf的gotoFault为Y，让vrrp实例进入fault状态，无法抢占写vip
                        my $modifyRes = modifyGotoFault($dbPort, $g_vrrpins, $wvip,"Y");
                        if ($modifyRes) {
                            $log->info("$g_vrrpins goto 'Fault' success");
                        }else{
                            $log->error("$g_vrrpins goto 'Fault' failed");
                            $log->info("$g_vrrpins retry modify goto 'Fault'");
                            $modifyRes = modifyGotoFault($dbPort, $g_vrrpins, $wvip,"Y");
                        }
                    }
                    
                    # 设置失败标识
                    $failed = 1;

                    # 结束循环
                    last;
                }
                
                # 继续重试
                next;
            }
        }
        alarm 0;
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler 
    
    if ($@){
        undef $@;
    }
}

# 删除文件锁
$log->debug("start removing $lockFile");
my $delFileLock = cleanFileLock($lockFile);
if ( !$delFileLock ){
    $log->error("remove $lockFile failed");
}else{
    $log->debug("remove $lockFile success");
}
$log->info("=  end($flag) =");

# 结束
if ( $failed == 1 ){
    exit 1;
}

exit 0;

