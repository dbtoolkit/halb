# Description:  get mysql connection handle, contain reconnect mechanism
# Authors:  
#   zhaoyunbo

package MysqlConn;

use strict;
use warnings;
use DBI;
use Log::Log4perl;
use POSIX qw(:signal_h);

# @Description: 构造函数
# @Param: myconfObj
# @Return: $self
sub new {
    my ( $class, %args ) = @_;

    my $self = {};  # create a hash ref   
    my $log = Log::Log4perl->get_logger(""); # get logObject
    
    # 接收myconfObj对象
    for (qw(myconfObj)) {
        if ( $args{$_} ) {
            $self->{$_} = $args{$_};
        }
        else {
            $log->error("got no myconfObj.");
            die "got no $_!";
        }
    }

    bless( $self, $class );  # 声明对象类型
    return $self;   # 返回对象
}

#
# @Description:  获取mysql连接句柄
# @Param:  $host $port
# @Return:  成功返回$dbh  失败返回undef
sub dbConnect {
    my ( $self, %args ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 
    
    # 获取数据库连接参数
    $self->{dbHost} = $args{dbHost};  # needed 
    $self->{dbPort} = $args{dbPort};  # needed
    $self->{dbName} = $args{dbName} || $self->{myconfObj}->get('dbName');
    $self->{dbUser} = $args{dbUser} || $self->{myconfObj}->get('dbUser');
    $self->{dbPassword} = $args{dbPassword} || $self->{myconfObj}->get('dbPassword');
    
    $self->{retryType} = $args{retryType} || $self->{myconfObj}->get('retryType');
    
    $self->{dbDsn} = "DBI:mysql:database=$self->{dbName};host=$self->{dbHost};port=$self->{dbPort}";
    
    # DBI属性设置
    $self->{attribute} = {
        AutoCommit         => 1,
        RaiseError         => 1,
        PrintError         => 0,
        ShowErrorStatement => 1,
    };
    
    # debug message
    my $connStr = "dbHost=$self->{dbHost},dbPort=$self->{dbPort},dbName=$self->{dbName},";
        $connStr .= "dbUser=$self->{dbUser},dbPassword=$self->{dbPassword},dbDsn=$self->{dbDsn}";
    $log->debug("$connStr");

    # 获取重连次数
    my $retry = $self->{myconfObj}->get('dbConnectRetryNum');
    if ( !defined($retry) ) { 
        $log->warn("Get dbConnectRetryNum is undef, set dbConnectRetryNum=10");
        $retry = 10;
    }
    $log->debug("get dbConnectRetryNum is $retry"); 
    
    # 获取连接超时时间
    my $dbConnTimeout = $self->{myconfObj}->get('dbConnTimeout');
    if ( !defined($dbConnTimeout) ) {
        $log->debug("get dbConnTimeout is undef, set dbConnTimeout=10");
        $dbConnTimeout = 10;
    }
    $log->debug("get dbConnTimeout is $dbConnTimeout");
    
    # 失败重试机制 
    my $timeoutMsg = "CONN_TIMEOUT";    
    my $dbh;
    while ( !$dbh && $retry-- ) {
        
        # 超时处理
        my $sigset = POSIX::SigSet->new( SIGALRM ); # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
        my $oldaction = POSIX::SigAction->new();
        sigaction( SIGALRM, $action, $oldaction );  

        eval {
            # 设置超时时间                     
            alarm $dbConnTimeout;
            # 连接mysql
            $dbh = DBI->connect( 
                $self->{dbDsn}, 
                $self->{dbUser}, 
                $self->{dbPassword}, 
                $self->{attribute} 
            );
            alarm 0;  # cancel the alarm
        };
        alarm 0; # race condition protection
        sigaction( SIGALRM, $oldaction );  # restore original signal handler
        
        # 异常处理
        if ( $@ ) {
            if ( $@ =~ /$timeoutMsg/ ) {
                # 连接超时
                $log->error("connect mysql: $self->{dbHost}:$self->{dbPort} timeout");
            } else {
                # 其他异常
                $log->error($@);
                $log->error("connect mysql: $self->{dbHost}:$self->{dbPort} failed");
                if ( !$self->{retryType} ) {
                    $self->{retryType} = "sleep";
                }
                if ( lc $self->{retryType} eq "sleep" ) {
                    # 获取重试时间间隔
                    my $dbReconnWaitTime = $self->{myconfObj}->get('dbReconnectWaitTime'); 
                    if ( !defined($dbReconnWaitTime) ) {
                        $log->warn("Get \$dbReconnWaitTime is undef, set \$dbReconnWaitTime=3s.");
                        $dbReconnWaitTime = 3;
                    }
                    
                    $log->info("sleep $dbReconnWaitTime second");
                    
                    # 重试时间间隔
                    sleep $dbReconnWaitTime;    # sleep 
                    
                } elsif ( lc $self->{retryType} eq "nosleep" ) {
                    $log->info("mysql reconnect type is nosleep mode");
                } 
            }
            # 清理$@
            undef $@;
        }
    } 
    if ( !$dbh ) {
        $log->error("mysql: $self->{dbHost}:$self->{dbPort} get dbh failed");
        
        # 失败，返回undef
        return;
    }
    $log->debug("connect mysql: $self->{dbHost}:$self->{dbPort} success");
    $log->debug("$dbh");
    
    # 成功，返回dbh
    return $dbh;
}

# @Description:  关闭mysql连接
# @Param:  $dbh
# @Return:  无返回值
sub disConnect {
    my ( $self, $dbh ) = @_;
    
    # 判断连接句柄存在
    if ( defined($dbh) ) {
        my $log = Log::Log4perl->get_logger(""); 
        
        my $timeout = $self->{myconfObj}->get('disConnTimeout');    
        if (!defined($timeout)){
            $log->debug("get disConnTimeout failed, set it 3s");
            $timeout=3;
        }       
        
        # 超时处理
        my $timeoutMsg = "DES_TIMEOUT"; 
        my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
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
            if ( $@ =~ /$timeoutMsg/ ) {
                # 超时
                $log->error("disConnect mysql timeout");
            }else{
                # 其他异常
                $log->error("disConnect mysql failed");
            }
            undef $@;
        }
        $log->debug("disConnect mysql success");
    }
}

# perl包固定用法，"1"为true，告诉perl解释器包正确
1;

