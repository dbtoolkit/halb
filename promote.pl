#!/usr/bin/env perl
# Description:  MySQL提升入口
# Authors:  
#   zhaoyunbo

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl;
use Parallel::ForkManager;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/common";
use lib "$Bin/etc";
use Myconfig;
use ManagerUtil;
use OpStatus;
use Promote;

# 接收参数
my ($gVrrpInstType,$gVrrpInstName,$gVrrpInstState);
GetOptions(
    "type=s" => \$gVrrpInstType,
    "vrrpinstance=s" => \$gVrrpInstName,
    "state=s" => \$gVrrpInstState
);

# 检查参数值
my $options_needed;
( defined($gVrrpInstType) ) or $options_needed .= "--type is needed !\n";
( defined($gVrrpInstName) ) or $options_needed .= "--vrrpinstance is needed !\n";
( defined($gVrrpInstState) ) or $options_needed .= "--state is needed !\n";
if ( $options_needed ){  die( $options_needed . "\n" ); }
if ( !defined($gVrrpInstType) ) { die("get \$gVrrpInstType failed.\n"); }
if ( !defined($gVrrpInstName) ) { die("get \$gVrrpInstName failed.\n");  }
if ( !defined($gVrrpInstState) ) { die("get \$gVrrpInstState failed.\n"); }

# 初始化log4perl
my $halbLog = "/var/log/halb_promote_$gVrrpInstName.log";   # 切换日志文件
my $logConf = q(
    log4perl.rootLogger                = INFO, logfile
    log4perl.appender.logfile          = Log::Log4perl::Appender::File
    log4perl.appender.logfile.mode     = append
    log4perl.appender.logfile.layout   = Log::Log4perl::Layout::PatternLayout
    #log4perl.appender.logfile.layout.ConversionPattern = [%d{yyyy-MM-dd HH:mm:ss}] [%p] [%F{1}:%L %M]  %m%n
    log4perl.appender.logfile.layout.ConversionPattern = %d{yyyy-MM-dd HH:mm:ss}  [%p] [%M] %m%n
);
$logConf .= "log4perl.appender.logfile.filename = $halbLog";

Log::Log4perl->init( \$logConf );
my $log = Log::Log4perl->get_logger("promote");  # get logObject ref
if ( !$log ){
    Log::Log4perl->init( \$logConf );
} 

# 标识一次切换流程，方便查看日志 
my $flag = int(rand(100000));
$log->info("===============  begin($flag)  =================");

# 设置文件锁，防止脚本并行执行
my $lockFile="/tmp/promote_$gVrrpInstName.lock";
$log->info("start get lockfile");
my $flockStatus=getFileLock("$lockFile");
my $exitCode = $$flockStatus{'exitCode'};
if ($exitCode != 0){
    $log->error("promote.pl is running, do not promote");
    $log->info("==  end($flag) ==");
    # 获取文件锁失败，不做提升
    exit 0;
}
$log->info("get lockfile: $lockFile success");

$log->info("get vrrpinstance [type: $gVrrpInstType, name: $gVrrpInstName, state: $gVrrpInstState]");

# 创建对象
my $myconfObj = new Myconfig();
if ( !$myconfObj ) {
    my $myconfObj = new Myconfig();    
}

# 检查Myconfig.pm自动切换开启情况
$log->info( "start checking switchover" ); 
my $switchover = $myconfObj->get('switchover');
if ( !$switchover ) {
    $log->info("switchover is not set, do nothing");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");

    # $self->{switchover}不为on，不做提升
    exit 0;
} elsif ( $switchover && lc($switchover) ne "on" ) {
    $log->info("switchover is not 'on', do nothing");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");

    # $self->{switchover}不为on，不做提升
    exit 0;
}
$log->info("switchover is on, continue...");

# 获取高可用全局配置文件
$log->info( "start getting haconf data" ); 
my $haconfFile = $myconfObj->get('haconfFile');
if ( !defined($haconfFile) ) { 
    $log->error("get haconf failed");
    cleanFileLock($lockFile);
    $log->info("==  end($flag) ==");
    
    # 配置文件不存在，不做切换
    exit 0;
}

# 清空opStatus状态文件
$log->info("start cleaning opStatus file");
eval { 
    opStatus($myconfObj, "", "trunc"); 
};
if($@){
    $log->error("clean status failed");
    undef $@; 
}

# 解析高可用全局配置文件haconf
my @instHaconf = `grep -v "#" $haconfFile | egrep -wi $gVrrpInstName`;
chomp(@instHaconf);


# 并行进行提升操作
$log->info();
$log->info();
$log->info("*parallelPromote*:   start doing promote");
 
# 使用Parallel::ForkManager模块并行提升
my $maxProcs = 10;
my $pm = Parallel::ForkManager->new($maxProcs);

$log->info("*parallelPromote*:   doing promote parallel...");  
$log->info("*parallelPromote*:   use Parallel::ForkManager, maxProcs: $maxProcs"); 
    
# setup a callback when a child finishes up, so we can get it's exit code
$pm->run_on_finish(  sub { 
    my ( $pid, $exitCode, $target ) = @_;
    
    $log->info();
    
    $log->info("[instance:$target]  target finish parallelPromote exitCode: $exitCode, pid: $pid");

    $log->info();    
    if ( $exitCode == 1 ){   
        $log->info("[instance:$target]  promote success");     
    }elsif ( $exitCode == 2 ){
        $log->info("[instance:$target]  target is current master, no need promote"); 
                    
    }elsif ( $exitCode == 0 ){
        $log->error("[instance:$target]  promote failed"); 
    }
});

$pm->run_on_start(
    sub { 
        my ($pid, $target)=@_;
        $log->info();
        $log->info("[instance:$target]  target start parallelPromote, pid: $pid");
    }
);

# 遍历所有实例,进行提升
foreach my $line (@instHaconf){
    my ( $isMaintained, $dbType, $dbHome, $sidOrPort, $dgGroup, $haGroupIp, $gotoFault, 
                $vrrpInstName, $vip, $realServerGroupIp) = (split(/\s+/, $line));
    
    # 以实例端口号作为target名称
    my $target = $sidOrPort;
    my $pid = $pm->start($target) and next;
    
    # 提升结果标示
    my $exitCode;
    eval {
        $exitCode = &parallelPromote( $isMaintained, $dbType, $dbHome, 
            $sidOrPort, $dgGroup, $haGroupIp, $gotoFault, 
            $vrrpInstName, $vip, $realServerGroupIp);

        if ( $exitCode == 1 ){
            # 提升成功
            $pm->finish(1);
        } elsif ( $exitCode == 2 ){
            # 忽略提升
            $pm->finish(2);
        } elsif ( $exitCode == 0 ) {
            # 提升失败
            $pm->finish(0);    
        }
    };
    if ($@) {
        $log->error("[instance:$sidOrPort]  error in parallelPromote, $@");
        undef $@;
        $pm->finish(0);
    }
}

$pm->wait_all_children;
    

# 删除文件锁
$log->info("start removing $lockFile");
my $delFileLock = cleanFileLock($lockFile);
if (!$delFileLock){
    $log->error("remove $lockFile failed");
}else{
    $log->info("remove $lockFile success");
}

# 结束
$log->info("================  end($flag)    =======================");



######################## Subroutines ########################

# @Description: 并行提升
# @Param: $isMaintained, $dbType, $dbHome, $sidOrPort, 
#         $dgGroup, $haGroupIp, $gotoFault, $vrrpInstName, $vip, $realServerGroupIp
#         $oldMasterHost,$newMasterHost,$dbPort )
# @Return: 1:成功  2:忽略  0:失败
sub parallelPromote{
    my ( $isMaintained, $dbType, $dbHome, $sidOrPort, $dgGroup, $haGroupIp, $gotoFault, 
                $vrrpInstName, $vip, $realServerGroupIp) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $haconfData = "haconf: isMaintained=$isMaintained, dbType=$dbType, dbHome=$dbHome,";
    $haconfData .= "sidOrPort=$sidOrPort, dgGroup=$dgGroup, haGroupIp=$haGroupIp,";
    $haconfData .= "gotoFault=$gotoFault, vrrpInstanceName=$vrrpInstName,";
    $haconfData .= "vip=$vip, realServerGroupIp=$realServerGroupIp";
    $log->info("[instance:$sidOrPort]   $haconfData");    
    
    # 检查高可用全局配置文件haconf设置
    $log->info("[instance:$sidOrPort]  start checking haconf"); 
    
    if ( lc($isMaintained) eq "y" ){
        $log->info("[instance:$sidOrPort]  $gVrrpInstName haconf is in maintain mode, exit promote");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
        
        # 高可用配置为"维护模式", 不做提升 
        exit 0;
    }
    $log->info("[instance:$sidOrPort]  $gVrrpInstName haconf is in switchover mode"); 
    
    
    # 检查实例类型
    $log->info("[instance:$sidOrPort]  start checking dbtype"); 
    
    if ( lc($dbType) ne "mysql" ){
        $log->info("[instance:$sidOrPort]  $gVrrpInstName haconf dbtype is not mysql, exit promote");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
        
        # dbtype不为"mysql"，不做提升
        exit 0;
    }
    $log->info("[instance:$sidOrPort]  dbtype is $dbType, dbtype is correct"); 
    
    # 获取mysql实例端口，同一组mysql高可用，mysql实例端口必需相同
    $log->info("[instance:$sidOrPort]  start checking mysql instance port"); 
    my $dbPort;
    if ( $sidOrPort ){ 
        $dbPort = $sidOrPort; 
    } else { 
        $log->error("[instance:$sidOrPort]  check mysql instance port failed");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
    
        # 获取mysql实例端口失败，不做提升
        exit 0;
    }
    $log->info( "[instance:$sidOrPort]  check mysql instance port success" ); 
    
    # 两个节点组成高可用组: 主库和备主
    # haGroupIp = localDbHost,remoteDbHost
    
    # 获取高可用组当前节点的ip地址
    $log->info("[instance:$sidOrPort]  in HALB haGroupIp is (localDbHost,remoteDbHost)");
    $log->info("[instance:$sidOrPort]  start getting localDbHost"); 
    my $localDbHost = getIpAddr();
    if ( !$localDbHost ) {
        $log->error("[instance:$sidOrPort]  get localDbHost failed");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
        
        # 获取本地节点ip地址失败，不做提升
        exit 0;
    }
    $log->info("[instance:$sidOrPort]  get localDbHost = $localDbHost");
    
    # 获取高可用组另外一个节点的ip地址
    $log->info("[instance:$sidOrPort]  start getting remoteDbHost");   
    if ( !$haGroupIp ) {
        $log->error("[instance:$sidOrPort]  get \$haGroupIp failed, haconf file format error");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
        
        # 获取高可用组失败，不做提升 
        exit 0;
    }
    
    my ( $oneIp, $theOtherIp ) = ( split( /,/, $haGroupIp ) );
    if ( ($localDbHost ne $oneIp) && ($localDbHost ne $theOtherIp) ) {
        $log->error("[instance:$sidOrPort]  get wrong \$localDbHost, it is not in the same HA group");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
        
        # 本地节点不在高可用组中，不做提升
        exit 0;
    }
    my $remoteDbHost;
    if ( $oneIp eq $localDbHost ) { 
        $remoteDbHost = $theOtherIp; 
    } else { 
        $remoteDbHost = $oneIp; 
    }
    if ( !$remoteDbHost ) {
        $log->error("[instance:$sidOrPort]  get \$remoteDbHost failed");
        cleanFileLock($lockFile);
        $log->info("==  end($flag) ==");
    
        # 另一节点不在高可用组中，不做提升
        exit 0;
    } 
    $log->info("[instance:$sidOrPort]  get remoteDbHost = $remoteDbHost"); 
    
    # 打印高可用组实例ip和端口
    $log->info("[instance:$sidOrPort]  haGroup is: $localDbHost:$dbPort, $remoteDbHost:$dbPort");
    
    
    # 提升状态
    my $promoteCode;
    
    # 高可用切换
    if ( lc $gVrrpInstType eq "instance" ) {
        if ( lc $gVrrpInstState eq "master" ) {
            eval {
                # 声明待初始化从库数组
                my @slaves;
                # 若集群存在realserver，则把realserver加入数组 
                if ( $realServerGroupIp && lc $realServerGroupIp ne 'null' ) {
                    @slaves = (split(/,/, $realServerGroupIp));
                    $log->debug("[instance:$sidOrPort]  \$realServerGroupIp = $realServerGroupIp"); 
                }
                # 把旧主库加入数组
                if ( $remoteDbHost ) {
                    push (@slaves, $remoteDbHost);
                }
                $log->info();
                $log->info("[instance:$sidOrPort]  status is MASTER, would promote mysql to master");            
                $log->info("[instance:$sidOrPort]  $localDbHost:$dbPort start doing promote");
                $log->info();
                
                # 提升操作
                my $promoteObj = new Promote();            
                $promoteCode = $promoteObj->main($vrrpInstName,\@slaves,
                                          $remoteDbHost,$localDbHost,$dbPort);
                $log->info();
                # 返回值为1，表示提升成功
                if ( $promoteCode == 1 ){
                    $log->info("[instance:$sidOrPort]  $localDbHost:$dbPort promote success"); 
                # 返回值为2，表示忽略提升操作
                }elsif ( $promoteCode == 2 ){
                    $log->info("[instance:$sidOrPort]  $localDbHost:$dbPort is current master, no need promote"); 
                # 返回值为其它，表示提升失败
                }elsif ( $promoteCode == 0 ) {
                    $log->error("[instance:$sidOrPort]  $localDbHost:$dbPort promote failed");
                }          
            };
            if($@){
                undef $@;
            }       
        }
    }
    
    return $promoteCode;
}

######################## END Subroutines ########################

1;

