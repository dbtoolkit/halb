#!/usr/bin/env perl
# Description： 检查mysql服务健康, 脚本退出状态码说明: 0-正常  1-异常（触发主库切换）
# Authors:  
#   zhaoyunbo

use strict;
use warnings;
use Getopt::Long;
use File::Spec;
use Log::Log4perl;
use Parallel::ForkManager;
use DBI;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/common";
use lib "$Bin/etc";
use Myconfig;
use ManagerUtil;

# 以下情况是mysql活着，但mysql dbi连接错误，属于可控制，不切换主库
my @ALIVE_ERROR_CODES = (
    1040,    # ER_CON_COUNT_ERROR
    1042,    # ER_BAD_HOST_ERROR
    1043,    # ER_HANDSHAKE_ERROR
    1044,    # ER_DBACCESS_DENIED_ERROR
    1045,    # ER_ACCESS_DENIED_ERROR
    1129,    # ER_HOST_IS_BLOCKED
    1130,    # ER_HOST_NOT_PRIVILEGED
    1203,    # ER_TOO_MANY_USER_CONNECTIONS
    1226,    # ER_USER_LIMIT_REACHED
    1251,    # ER_NOT_SUPPORTED_AUTH_MODE
    1275,    # ER_SERVER_IS_IN_SECURE_AUTH_MODE
);

my $g_scriptname = $0;

# 接收参数
my ( $g_vrrpins, $g_total_timeout );
GetOptions(
    "vrrp-instance=s" => \$g_vrrpins,
    "total-timeout-seconds=i" => \$g_total_timeout
);
# 检查参数值
if ( ! defined($g_vrrpins) ) {
    print "--vrrp-instance= is needed\n";
    exit 0;
}
if ( ! defined($g_total_timeout) ) {
    print "--total-timeout-seconds= is needed\n";
    exit 0;
}


# 初始化log4perl
my $checkLog = "/var/log/check_mysql_$g_vrrpins.log";
my $logConf = q(
    log4perl.rootLogger                = DEBUG, logfile
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
$log->info("===============  begin($flag)  =================");

# 设置文件锁，防止脚本并行执行
my $lockFile="/tmp/check_mysql_$g_vrrpins.lock";
$log->info("start get lockfile");
my $flockStatus=getFileLock("$lockFile");
my $exitCode = $$flockStatus{'exitCode'};
if ($exitCode != 0){
    $log->error("$g_scriptname is running");
    exit 0;
}
$log->info("get lockfile: $lockFile success");

# 创建Myconfig对象
my $myconfObj = new Myconfig(); 
if ( !$myconfObj ) { 
    $myconfObj = new Myconfig(); 
}

# 获取和检查配置参数
# 数据库主机
my $g_host = $myconfObj->get('checkHost');
if ( !$g_host ) { 
    $log->error("get checkHost failed.");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
}

# 数据库用户名
my $g_username = $myconfObj->get('checkUser');
if ( !$g_username ) { 
    $log->error("get checkUser failed.");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
}

# 数据库密码
my $g_password = $myconfObj->get('checkPassword');
if ( !$g_password ) { 
    $log->error("get checkPassword failed.");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
}

# 高可用全局配置文件
my $g_inscnf = $myconfObj->get('haconfFile');
if ( !$g_inscnf ) { 
    $log->error("get db haconfFile failed.");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
}
# 检查/etc/keepalived/haconf文件是否存在，若不存在，则不做切换 
if (! open my $FH, "<", $g_inscnf ){
    $log->error("Cannot open ha config file $g_inscnf");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
} else {
    close( $FH );
}

# 检查Myconfig.pm自动切换开启情况
$log->info( "start checking switchover" ); 
my $switchover = $myconfObj->get('switchover');
if ( !$switchover ) {
    $log->info("switchover is not set, do nothing");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
} elsif ( $switchover && lc($switchover) ne "on" ) {
    $log->info("switchover is not 'on', do nothing");
    cleanFileLock($lockFile);
    $log->info("remove $lockFile success");
    $log->info("==  end($flag) ==");
    exit 0;
}

# 解析配置文件
my @haconf_data = `grep -v "#" $g_inscnf | grep -w $g_vrrpins`;
chomp(@haconf_data);

foreach my $line (@haconf_data){
    $line =~ s/^\s+//g;
    my ($is_maintained,$dbtype,$dbhome,$sidorport,$group,$ipgrp,$gotofault,$vrrpins,$vip,$realgrp) = (split(/\s+/, $line));
        
    # vrrp实例不匹配，正常退出
    if ( lc($g_vrrpins) ne lc($vrrpins) ) {
        $log->info("$g_host:$sidorport $g_vrrpins is not $vrrpins");
        
        my $delLock = cleanFileLock($lockFile);
        if (!$delLock){
            $log->error("remove $lockFile failed");
        }else{
            $log->info("remove $lockFile success");
        }
        $log->info("==  end($flag) ==");
        
        exit 0;
    }
    # 类型不为mysql，正常退出
    if ( lc($dbtype) ne "mysql" ) {
        $log->info("$g_host:$sidorport $dbtype is not mysql");
        
        my $delLock = cleanFileLock($lockFile);
        if (!$delLock){
            $log->error("remove $lockFile failed");
        }else{
            $log->info("remove $lockFile success");
        }
        $log->info("==  end($flag) ==");
        
        exit 0;
    }
    # 高可用维护模式为"Y"，正常退出
    if ( lc($is_maintained) eq "y" ) {
        $log->info("$g_host:$sidorport in maintain mode");
        
        my $delLock = cleanFileLock($lockFile);
        if (!$delLock){
            $log->error("remove $lockFile failed");
        }else{
            $log->info("remove $lockFile success");
        }
        $log->info("==  end($flag) ==");
        
        exit 0;
    }
    # 若gotofault为"Y"，则进行主从切换
    if( lc($gotofault) eq "y" ) {
        $log->error("$g_host:$sidorport gotofault is Y");
        $log->error("$g_host:$sidorport will switchover");
        
        my $delLock = cleanFileLock($lockFile);
        if (!$delLock){
            $log->error("remove $lockFile failed");
        }else{
            $log->info("remove $lockFile success");
        }
        $log->info("==  end($flag) ==");
        
        exit 1;
    }
}

# 检查失败重试次数
my $g_retry = $myconfObj->get('checkMysqlRetryTimes');
if ( !$g_retry ) { 
    $log->error("get checkMysqlRetryTimes failed.");
    $g_retry=10;
}

# 默认探测mysql可写操作
my $g_chk_write = $myconfObj->get('checkMysqlWrite');
if ( ! defined($g_chk_write) ) {
    $log->warn("checkMysqlWrite is not set, set it 1");
    $g_chk_write = 1;
}

# 计算同一个虚拟ip下的mysql实例数量
my $count = `grep -v "#" $g_inscnf | grep -wc $g_vrrpins`;
chomp($count);
if ( $count > 0 ) {
    $log->debug("total number of mysql instance in vrrp instance $g_vrrpins is: $count");
} else {
    $log->error("no mysql instance in vrrp instance: $g_vrrpins");
    exit 0;
}

# 计算单次失败检查重试时间间隔
my $g_sleeptime = 1;

# 计算单次检查超时时间
my $timeout = $g_total_timeout/$g_retry - $g_sleeptime; 
$timeout = sprintf("%d", $timeout);
$timeout = 1 if( $timeout < 1 );
$log->debug("check timeout is: $timeout");


# 实例总数
my $total_instance_num = scalar(@haconf_data);

# 检查脚本退出码
my $exit_code = 0;

# 并行进行检查操作
$log->info("*parallelCheck*:   start doing check");
 
# 使用Parallel::ForkManager模块并行提升
my $maxProcs = 20;
my $pm = Parallel::ForkManager->new($maxProcs);

$log->info("*parallelCheck*:   doing checks parallel...");  
$log->info("*parallelCheck*:   use Parallel::ForkManager, maxProcs: $maxProcs");

# 所有检查结果
my %check_result = ();

# setup a callback when a child finishes up, so we can get it's exit code
$pm->run_on_finish(  sub { 
    my ( $pid, $exitCode, $target ) = @_;
    
    $log->info("[instance:$target]  target finish parallelCheck exitCode: $exitCode, pid: $pid");
    if ( $exitCode == 1 ){
        $check_result{$target} = 1;
        $log->info("[instance:$target]  check success");           
    }elsif ( $exitCode == 0 ){
        $check_result{$target} = 0;
        $log->error("[instance:$target]  check failed"); 
    }
});

$pm->run_on_start(
    sub { 
        my ($pid, $target) = @_;
        $log->info("[instance:$target]  target start parallelCheck, pid: $pid");
    }
);

foreach my $line (@haconf_data){
    $line =~ s/^\s+//g;
    my ($is_maintained,$dbtype,$dbhome,$sidorport,$group,$ipgrp,$gotofault,$vrrpins,$vip,$realgrp) = (split(/\s+/, $line));
        
    # 以实例端口号作为target名称
    my $target = $sidorport;
    my $pid = $pm->start($target) and next;
    
    # 单个实例检查结果
    my $exitCode;
    eval {
        $exitCode = &parallelCheck($is_maintained,$dbtype,$dbhome,$sidorport,
            $group,$ipgrp,$gotofault,$vrrpins,$vip,$realgrp);

        if ( $exitCode == 1 ){
            # 检查成功
            $pm->finish(1);
        } elsif ( $exitCode == 0 ) {
            # 检查失败
            $pm->finish(0);    
        }
    };
    if ($@) {
        $log->error("[instance:$sidorport]  error in parallelCheck, $@");
        undef $@;
        $pm->finish(1);
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

# 根据检查结果设置退出状态码
my $all_instance_success = 0;
foreach my $target (keys %check_result){
    my $success = $check_result{$target};
    if ($success > 0){
        $all_instance_success = $all_instance_success + 1;
    }
}

if ( $all_instance_success > 0 ) {
    # 正常
    $log->info("one of mysql instance is ok");
    $exit_code = 0;
} else {
    # 异常
    $log->error("All mysql instance is bad");
    $exit_code = 1;
}

$log->info("================  end($flag)    =======================");

# 结束    
exit($exit_code);



######################## Subroutines ########################

# @Description: 并行检查
# @Param: $isMaintained, $dbType, $dbHome, $sidOrPort, 
#         $dgGroup, $haGroupIp, $gotoFault, $vrrpInstName, $vip, $realServerGroupIp
#         $oldMasterHost,$newMasterHost,$dbPort )
# @Return: 1:成功  2:忽略  0:失败
sub parallelCheck{
    my ($is_maintained,$dbtype,$dbhome,$sidorport,$group,$ipgrp,$gotofault,$vrrpins,$vip,$realgrp) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 获取mysql实例端口
    my $g_port = $sidorport;
       
    # 获取mysql实例写vip
    my $wvip = `grep -v "#" $g_inscnf | egrep -w $sidorport | grep -wi $vrrpins | awk {'print \$9'}| uniq`;
    chomp($wvip);
    $log->debug("get write vip: $wvip");
    
    # 检查过程    
    my ($dbh,$dbi_errcode); 
    
    # 单个mysql实例检查结果标识
    my $single_instance_success = 0;  
    
    # 设置检查失败重试机制
    for ( my $i=0; $i<$g_retry; $i++ ) {    
        
        $log->debug("=$g_host:$g_port the check  ($i) times=");
        
        my $timeout_msg = "CHK_TIMEOUT"; 
        my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeout_msg; },$sigset,);
        my $oldaction = POSIX::SigAction->new();
        sigaction( SIGALRM, $action, $oldaction ); 
        
        eval {
            # 设置超时时间
            alarm $timeout;
            
            # 1. 检查mysql连接
            ($dbh,$dbi_errcode) = &mysql_connect( $g_host, $g_port, $g_username, $g_password );
            if ( ! defined($dbh) ) {               
                if (defined($dbi_errcode)){
                    die $dbi_errcode;
                }else{
                    die "connect failed";
                }
            }
             
            # disable session binary log
            &mysql_execute( $dbh, "set session sql_log_bin=0" );
             
            # 2. 检查mysql可读
            my $showsql = "show variables like 'server_id%'";
            my $query_result;
            $query_result = &mysql_query( $dbh, $showsql );
            if ( ! $query_result || ! %{$query_result} ) {
                die "check read_only failed";
            }
            
            # 3. 检查mysql可写
            if ( $g_chk_write == 1 ) {
                # disable session binary log
                &mysql_execute( $dbh, "set session sql_log_bin=0" );
                    
                # test write
                &mysql_execute( $dbh, "create database if not exists mysql_identity" );
                &mysql_execute( $dbh, "drop table if exists mysql_identity.chk_masterha" );
                &mysql_execute( $dbh, "create table if not exists mysql_identity.chk_masterha (id int) engine=innodb" );
                &mysql_execute( $dbh, "insert into mysql_identity.chk_masterha(id) values (1)" );
                
                my $query_result;
                $query_result = &mysql_query( $dbh, "select id from mysql_identity.chk_masterha" );
                if ( ! $query_result || ! %$query_result ) {
                    die "check write failed";
                }
                &mysql_execute( $dbh, "drop table if exists mysql_identity.chk_masterha" );
                
            } else {
                $log->info("check write no set");
            }
                    
            # 关闭mysql连接
            if ($dbh) { &mysql_disconnect($dbh); }
    
            # 设置检查结果为成功
            $single_instance_success = 1;
            
            alarm 0;
        };
        alarm(0);
        sigaction( SIGALRM, $oldaction );  # restore original signal handler
    
        # 若成功，结束循环
        if ($single_instance_success == 1){
            # 结束循环
            last;
        }
            
        # 若存在异常，则
        if ( $@ ){        
            if ( $@ =~ /$timeout_msg/ ) {
            # 发生超时
                $log->error("$g_host:$g_port timeout: $@");
            
            } elsif ( grep ( $_ == $@, @ALIVE_ERROR_CODES )> 0 ) {
            # mysql活着，但dbi连接错误, 此类问题不做主从切换    
                $log->info("$g_host:$g_port  ignored: dbi err is $@");
                
                # 最后一次检查
                if ( $i == ($g_retry-1) ){            
                    # 设置检查结果为成功
                    $single_instance_success = 1;
                    
                    # 结束循环
                    last;
                }       
            } else{
            # 其它错误
                $log->error("$g_host:$g_port failed: $@");
            }
              
            # 关闭mysql连接
            if ($dbh) { &mysql_disconnect($dbh); }    
    
            # 最后一次检查
            if ( $i == ($g_retry-1) ){
                # 结束循环            
                last;
            }             
            
            sleep $g_sleeptime;
            # 重新检查
            next;       
        }
    } # end for
    
    # 关闭mysql连接
    if ($dbh) { &mysql_disconnect($dbh); }

    # 根据检查结果设置退出状态码
    if ( $single_instance_success == 1 ) {
        # 正常
        $log->info("$g_host:$g_port is ok");
    } else {
        # 异常
        $log->error("$g_host:$g_port is bad");
    }
    
    return $single_instance_success;
}

sub mysql_connect {
    my ( $host, $port, $user, $pass ) = @_;
    my $dsn = "DBI:mysql:host=$host;port=$port";
    $log->debug("$dsn, user:$user, pass:****");
    
    my $err;
    my $dbh = DBI->connect( $dsn, $user, $pass, { PrintError => 0, RaiseError => 0, 
                           AutoCommit => 1} );
                    
    if ($DBI::err){
        $err = $DBI::err;
    }
    
    return ($dbh, $err);
}

sub mysql_query {
    my ( $dbh, $query ) = @_;
    $log->debug("$query");
    my $sth = $dbh->prepare($query);
    my $res = $sth->execute;
    return undef unless ($res);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    
    return $row;
}

sub mysql_execute {
    my ( $dbh, $sql ) = @_;
    $log->debug("$sql");
    my $sth = $dbh->do($sql);

    return;
}

sub mysql_disconnect {
    my ( $dbh ) = @_;
    if ( defined($dbh) ) {
        eval {
            $log->debug("disconnect");
            $dbh->disconnect();         
        };
        if($@){
            undef $@;
        }
    }
}


######################## END Subroutines ########################

