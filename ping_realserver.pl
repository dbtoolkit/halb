#!/usr/bin/env perl
# Description： 连接mysql检查从库健康, 脚本退出状态码说明: 0-正常  1-异常（摘除realserver）
# Authors:  
#   zhaoyunbo

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Log::Log4perl;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/etc";
use Myconfig;

# init log4perl
my $checkLog = "/var/log/check_realserver.log";
my $logConf = q(
    log4perl.rootLogger                = INFO, logfile
    log4perl.appender.logfile          = Log::Log4perl::Appender::File
    log4perl.appender.logfile.mode     = append
    log4perl.appender.logfile.layout   = Log::Log4perl::Layout::PatternLayout
    log4perl.appender.logfile.layout.ConversionPattern = %d{yyyy-MM-dd HH:mm:ss}  [%p] [%M] %m%n
);
$logConf .= "log4perl.appender.logfile.filename = $checkLog";

Log::Log4perl->init( \$logConf );
my $log = Log::Log4perl->get_logger("");  # get logObject ref
if ( !$log ){
    Log::Log4perl->init( \$logConf );
} 

my ($g_host,$g_port,$g_masterhost,$g_version,$g_chk_dbconn_only);
# 接收参数
GetOptions(
    "host=s" => \$g_host,     # realserver ip
    "port=i" => \$g_port,     # realserver port
    "masterhost=s" => \$g_masterhost,   # 主库ip
    "version" => \$g_version,
    "chk_dbconn_only=i" => \$g_chk_dbconn_only,  # 只检查mysql连接，该值目的是检查主库  
);

if( $g_version ) {
    system("pod2text -i 0 -s $0|grep ping_realserver");
    exit 0;
}

# 检查参数值
if (!defined($g_host)){
    $log->error("--host= is needed");
    exit 0;
}
$log->debug("g_host=$g_host");

if (!defined($g_port)){
    $log->error("--port= is needed");
    exit 0;
}
$log->debug("g_port=$g_port");

if (!defined($g_masterhost)){
    $log->error("--masterhost= is needed");
    exit 0;
}
$log->debug("g_masterhost=$g_masterhost");

if (!defined($g_chk_dbconn_only)){
    # 默认检查策略
    $g_chk_dbconn_only = 0;
    $log->debug("g_chk_dbconn_only=0");
}

# 创建Myconfig对象
my $myconfObj = new Myconfig(); 
if ( !$myconfObj ) { 
    $myconfObj = new Myconfig(); 
}

# 连接realserver的mysql用户名
my $g_username = $myconfObj->get('dbUser');
if ( !$g_username ) { 
    $log->error("get db user failed.");
    exit 0;
}

# 连接realserver的mysql密码
my $g_password = $myconfObj->get('dbPassword');
if ( !$g_password ) { 
    $log->error("get db password failed.");
    exit 0;
}

# 允许从库最大复制延迟时间
my $slave_lag = $myconfObj->get('rsMaxSlaveLag');
if ( !$slave_lag ) {
    $log->error("get rsMaxSlaveLagg failed, set it 300s");
    $slave_lag = 300;
}

# 检查超时时间
my $g_timeout = $myconfObj->get('checkRsTimeout');
if ( !$g_timeout ) { 
    $log->error("get checkRsTimeout failed, set it 10s");
    $g_timeout = 10;
}

# 检查失败重试次数
my $retry_times = $myconfObj->get('checkRsRetryTimes');
if ( !$retry_times ) { 
    $log->error("get checkRsRetryTimes failed, set it 3");
    $retry_times = 3;
}

# 检查失败重试等待时间间隔
my $wait_interval = $myconfObj->get('checkRsIntervalTime');
if ( !$wait_interval ) { 
    $log->error("get checkRsIntervalTime failed, set it 2s");
    $wait_interval = 2;
}


# 检查从库健康
my $success = 0;  # 检查结果

# 失败重试机制
for ( my $i=0; $i<$retry_times; $i++ ) {

    my $timeout_msg = "RS_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeout_msg; }, $sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );
    
    my $dbh;
    eval {
        # 设置检查超时
        alarm $g_timeout;
        
        # 连接mysql
        $dbh = &mysql_connect( $g_host, $g_port, $g_username, $g_password, 10 );
        if ( !$dbh ) {
            $log->error("$g_host:$g_port connect failed");
            die "$g_host:$g_port error";
        }
        
        # 从库需要检查以下项目
        if ($g_chk_dbconn_only == 0){      
            # 检查show slave status
            my $slave_status = &mysql_query( $dbh, "SHOW SLAVE STATUS" );
            
            # show slave status为空，realserver不是从库
            if ( !$slave_status || !%{$slave_status} ) {
                $log->error("$g_host:$g_port no slave status found");
                die "$g_host:$g_port error";
            }
            
            # show slave status不为空
            if ( $slave_status && %{$slave_status} ) {    
                # 把$slave_status的key转换为小写
                $slave_status = { map { lc($_) => $slave_status->{$_} } keys %{$slave_status} };
                
                # 检查复制关系
                if ( exists $slave_status->{master_host} && $slave_status->{master_host} ne $g_masterhost ) {
                    $log->error("$g_host:$g_port repl master_host is wrong, master_host should be $g_masterhost");
                    die "$g_host:$g_port error";
                }                    
                # 检查从库复制线程
                if ( exists $slave_status->{slave_io_running} && lc($slave_status->{slave_io_running}) eq 'no' ) {
                    $log->error("$g_host:$g_port slave_io_running is $slave_status->{slave_io_running}");
                    die "$g_host:$g_port error";
                }
                if ( exists $slave_status->{slave_sql_running} && lc($slave_status->{slave_sql_running}) ne 'yes' ) {
                    $log->error("$g_host:$g_port slave_sql_running is $slave_status->{slave_sql_running}");
                    die "$g_host:$g_port error";
                }
                # 检查从库复制延迟
                if ( exists $slave_status->{seconds_behind_master} && $slave_status->{seconds_behind_master} >= $slave_lag ) {
                    $log->error("$g_host:$g_port Slave lag $slave_status->{seconds_behind_master}s");
                    die "$g_host:$g_port error";
                }
            }
        }
        
        # realserver正常
        $log->info("$g_host:$g_port is ok");
                
        # 断开mysql连接
        if ($dbh){ &mysql_disconnect($dbh, 5); }

        # 检查成功
        $success = 1;
                
        alarm 0; 
    }; 
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler 
    
    if ($success == 1){
        # 成功，正常退出
        exit 0;
    }

    # 若存在异常
    if ($@){
        $log->error("$g_host:$g_port is bad");
        if ($@ =~ /$timeout_msg/) {
            $log->error("$g_host:$g_port ping_realserver.pl timeout");
        }
                
        # 最后一次检查还失败
        if ( $i == ($retry_times-1) ){ 
         
            # 杀掉mysql应用账号连接会话，避免业务读到脏数据
            if ($dbh){
                &kill_mysql_session($dbh,$g_host,$g_port,5);
                &mysql_disconnect($dbh, 5);
            }
            
            # 失败，异常退出
            exit 1;
        }
        
        # 断开mysql连接
        if ($dbh){
            &mysql_disconnect($dbh, 5);
        }
        # 重试等待        
        sleep $wait_interval;
        # 重试
        next;
    }    
}


######################## Subroutines ########################

# @Description:  获取mysql连接句柄
# @Param:  $host, $port, $user, $pass, $timeout
# @Return:  成功返回$dbh  失败返回undef
sub mysql_connect {
    my ( $host, $port, $user, $pass, $timeout ) = @_;

    my $log = Log::Log4perl->get_logger("");
    
    if (!$timeout){
        $timeout = 10;
    }
    my $dsn = "DBI:mysql:host=$host;port=$port";
    
    # 超时处理
    my $timeout_msg = "DBCONN_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeout_msg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    
    my $dbh;
    # 把可能发生异常的代码放在eval中
    eval {
        alarm $timeout;
        # 连接mysql  
        $dbh = DBI->connect( $dsn, $user, $pass, { 
                          PrintError => 0,RaiseError => 1,AutoCommit => 1 });
        alarm 0;
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    # 如果有异常, $@不为空
    if ( $@ ) {
        if ( $@ =~ /$timeout_msg/ ) {
            $log->error("mysql_connect timeout");
        }else{
            $log->error("mysql_connect failed");
        }
        undef $@;
        
        # 失败返回undef
        return;
    }
    $log->debug("mysql_connect success");
    
    # 成功返回
    return $dbh;
}

# @Description:  关闭mysql连接
# @Param:  $dbh $timeout
# @Return:  无返回值
sub mysql_disconnect {
    my ( $dbh, $timeout ) = @_;

    my $log = Log::Log4perl->get_logger("");
        
    # 判断连接句柄存在
    if ( defined($dbh) ) {        
        if (!$timeout){
            $log->warn("get timeout failed, set it 10s");
            $timeout = 10;
        } 
        
        # 超时处理
        my $timeout_msg = "DIS_TIMEOUT"; 
        my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeout_msg; },$sigset,);
        my $oldaction = POSIX::SigAction->new();
        sigaction( SIGALRM, $action, $oldaction ); 
        eval {
            alarm $timeout;
            # 关闭mysql连接
            $dbh->disconnect;
            alarm 0; 
        };
        alarm 0; # race condition protection
        sigaction( SIGALRM, $oldaction );  # restore original signal handler
        
        # 异常处理
        if ( $@ ) {
            if ( $@ =~ /$timeout_msg/ ) {
                $log->error("mysql_disconnect timeout");
            }else{
                $log->error("mysql_disconnect failed");
            }
            undef $@;
        }
        $log->debug("mysql_disconnect success");
    }
}

# @Description:  执行查询语句
# @Param:  $dbh  $sql
# @Return:  查询结果
sub mysql_query {
    my ( $dbh, $sql ) = @_;
    $log->debug("$sql");
    
    my $sth = $dbh->prepare($sql);
    my $res = $sth->execute;
    return undef unless ($res);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    
    return $row;
}

# @Description:  执行更新语句
# @Param:  $dbh  $sql
# @Return:  无返回值
sub mysql_execute {
    my ( $dbh, $sql ) = @_;
    $log->debug("$sql");
    
    my $sth = $dbh->do($sql);
}

# @Description:  杀mysql连接会话线程
# @Param:  $dbh, $dbHost, $dbPort, $timeout
# @Return: 成功返回$killCount 失败返回undef
sub kill_mysql_session{
    my ( $dbh, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 
    
    # 超时处理    
    my $timeoutMsg = "KMS_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    
    # 被杀会话数量
    my $killCount = 0;
    eval {    
        alarm $timeout; 
        my $sql = "show processlist";     
        my $sth=$dbh->prepare($sql);
        $sth->execute();
        while ( my $rowRef = $sth->fetchrow_arrayref() ) {        
            if ( $rowRef ){
                my $sessionId = $rowRef->[0];
                my $sessionUser = $rowRef->[1];
                chomp($sessionId);
                chomp($sessionUser);
                # 不杀管理账号用户线程
                if ( $sessionUser && $sessionUser !~ /repadm|slave|event_scheduler|system|root|mysqlha|halb/i ){
                    if ($sessionId){
                        $log->info("$dbHost:$dbPort kill session: user->$sessionUser,id->$sessionId");
                        my $killSql="kill $sessionId";           
                        my $sth_n=$dbh->prepare($killSql);
                        $sth_n->execute();
                        $sth_n->finish();
                        $killCount++;
                    }  
                }
            }
        }
        $sth->finish();

        alarm 0; 
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler
    
    # 异常处理
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort kill session timeout");
        }else{
            $log->error("$dbHost:$dbPort kill session failed");
        }
        undef $@;
        
        # 返回undef            
        return;
    }
    
    if ($killCount >0){
        $log->info("$dbHost:$dbPort kill session success");
    }
    
    # 返回被杀会话数量
    return $killCount;
}


######################## END Subroutines ########################


