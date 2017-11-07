# Description:  manage mysql server, mysql replication 
# Authors:  
#   zhaoyunbo

package MysqlManager;

use strict;
use warnings;

use Log::Log4perl;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/../etc";
use Myconfig;
use MysqlConn;


# @Description: 构造函数
# @Param: 
# @Return: 对象
sub new {
    my ( $class, %args ) = @_;
    my $self = {};  # create a hash ref
    my $log = Log::Log4perl->get_logger("");  # get logObject
    
    # 接收myconfObj、mysqlConnObj对象
    for (qw(myconfObj mysqlConnObj)) {
        if ( $args{$_} ) {
            $self->{$_} = $args{$_};
            $log->debug("Get $_ success."); # debug mode
        } else {
            $log->error("Get $_ failed.");
            die "Get no $_.";
        }
    }    

    bless ( $self, $class );  # 声明对象类型
    return $self;  # 返回对象
}

# @Description: 获取SHOW SLAVE STATUS数据
# @Param: $dbHost, $dbPort, $timeout
# @Return: 成功返回$slaveStatus 失败返回undef

sub getSlaveStatus {
    # $self为当前对象，perl中传递给函数的所有参数值放在@_数组中
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    # Log::Log4perl为单例模式，这里直接拿logger对象
    my $log = Log::Log4perl->get_logger(""); 
    
    # 连接mysql
    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        
        # 连接失败,返回undef
        return;
    }
    
    # perl自身alarm函数无法处理DBI连接超时，需要使用POSIX::SigAction处理超时   
    my $timeoutMsg = "SSTAT_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    
    my $slaveStatus;
    
    # 把可能存在异常的代码放在eval中, eval();if($@){} 类似try...catch...
    eval {
        # 设置超时时间
        alarm $timeout;  
        
        my $sql = "SHOW SLAVE STATUS";
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $slaveStatus = $sth->fetchrow_hashref();
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);
                       
        if ( !$slaveStatus || scalar keys %{$slaveStatus} < 1 ) {    
            $log->error("$dbHost:$dbPort get $sql failed, it is not a slave");  
            return;
        }
        if ( $slaveStatus && %{$slaveStatus} ) {    
            # lowercase the keys
            $slaveStatus = { map { lc($_) => $slaveStatus->{$_} } keys %{$slaveStatus} };  
        }
        # 解除超时控制
        alarm 0;    
    };
    # 解除超时控制
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # 恢复原来signal handler
    
    # 异常捕获处理，若上面eval代码块有异常抛出，则异常信息会存储在$@
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            # 超时
            $log->error("$dbHost:$dbPort execute getSlaveStatus timeout");
        }else{
            # 其它失败
            $log->error("$dbHost:$dbPort execute getSlaveStatus failed");
        }
        # 清理$@    
        undef $@;
        # 关闭mysql连接      
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
        
        # 失败，返回undef          
        return;
    }
    
    $log->info("$dbHost:$dbPort execute getSlaveStatus success");     
    
    # 成功        
    return $slaveStatus;
}


# @Description: get replication binlog file and pos
# @Param: ( $status, $dbHost, $dbPort )
# @Return: $replicationPos or undef
sub getReplicationPos {
    my ( $self, $status, $dbHost, $dbPort ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 
    
    my $replicationPos = {};  # create a hash ref 
   
    # get data from show master status
    if ( $status && %{$status} 
      && exists $status->{file} 
      && exists $status->{position} ) {  
        $replicationPos->{file} = $status->{file};
        $replicationPos->{position} = $status->{position};     
   
    # get data from show slave status
    } else {
        $replicationPos->{file} = $status->{relay_master_log_file};
        $replicationPos->{position} = $status->{exec_master_log_pos};
    }
    $log->info("$dbHost:$dbPort replication pos: ");
    $log->info("file=$replicationPos->{file}, position=$replicationPos->{position}");
     
    return $replicationPos;
}
    
    
# @Description: get SHOW MASTER STATUS 
# @Param: ($dbHost,$dbPort,$timeout)
# @Return: $masterStatus or undef
sub getMasterStatus {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 
    
    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }

    my $sql = "SHOW MASTER STATUS";        
    my $timeoutMsg = "READONLY_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    
    my ($masterStatus, $masterLogFile, $masterLogPos);
    eval {
        alarm $timeout;
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $masterStatus = $sth->fetchrow_hashref();
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);
        
        if ( !$masterStatus || scalar keys %$masterStatus < 2 ) {
            $log->error("$dbHost:$dbPort execute $sql failed, bin_log not turn on");
            
            return;
        }
        if ( $masterStatus && %{$masterStatus} ) {
            $masterStatus = { map { lc($_) => $masterStatus->{$_} } keys %$masterStatus }; 
            if ( exists $masterStatus->{file} && exists $masterStatus->{position} ) {
                $masterLogFile = $masterStatus->{file};
                $masterLogPos = $masterStatus->{position};
            }
        }
       alarm 0;    
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler
    
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort execute $sql timeout");
        }else{
            $log->error("$dbHost:$dbPort execute $sql failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                    
        return;
    } 
    $log->info("$dbHost:$dbPort binlog pos: File=$masterLogFile, Position=$masterLogPos");    
      
    return $masterStatus;
}


# @Description: execute SQL
# @Param: ( $dbHost, $dbPort, $sql, $timeout )
# @Return: 1 or undef
sub executeSql {
    my ( $self, $dbHost, $dbPort, $sql, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
           
    my $timeoutMsg = "SQL_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
        
    eval {
        alarm $timeout;   
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0; 
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort execute $sql timeout");
        }else{
            $log->error("$dbHost:$dbPort execute $sql failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); }
                   
        return;
    }
    
    $log->info("$dbHost:$dbPort execute $sql success");
    
    return 1; 
}

#
# @Description: use 'CHANGE MASTER TO' command to change master
# @Param: $dbh $dbHost
# @Return: 1 or undef
#
sub changeMasterTo {
    my ( $self,$slaveHost,$masterHost,$dbPort,$masterLogFile,$masterLogPos,$timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $slaveHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $slaveHost:$dbPort failed");
        return;
    }

	# get repl user && password
	my $replUser = $self->{myconfObj}->get('dbUser');
	if ( !$replUser ) { 
		$log->error("$slaveHost:$dbPort get repl user failed");
		die ("$slaveHost:$dbPort get repl user failed\n");
	}
	my $replPassword = $self->{myconfObj}->get('dbPassword');
	if ( !$replPassword ) { 
		$log->error("$slaveHost:$dbPort get repl password failed");
		die ("$slaveHost:$dbPort get repl password failed\n"); 
	}
	$log->debug("$slaveHost:$dbPort get repluser=$replUser replPassword=$replPassword"); 
	
	# change master to     
    my $sql = "CHANGE MASTER TO MASTER_HOST='" . $masterHost 
            . "',MASTER_PORT=" . $dbPort
            . ",MASTER_USER='" . $replUser
            . "',MASTER_PASSWORD='" . $replPassword
            . "',MASTER_LOG_FILE='" . $masterLogFile 
            . "',MASTER_LOG_POS=" . $masterLogPos;
            
    $log->info("$slaveHost:$dbPort execute changeMasterTo, SQL: ");
    $log->info();
    $log->info("  =======     Change master to SQL:    ========   ");
    $log->info("$sql;");
    $log->info();
    $log->info();
    my $timeoutMsg = "CMT_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    eval { 
        alarm $timeout;  
        $dbh->do($sql);
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0;  
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$slaveHost:$dbPort execute changeMasterTo timeout");
        }else{
            $log->error("$slaveHost:$dbPort execute changeMasterTo failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                     
        return;
    }
    
    $log->info("$slaveHost:$dbPort execute changeMasterTo success");    
    
    return 1; 
}


# @Description: check BackupNode replication config
# @Param: ( $backupHost, $masterHost, $dbPort, $timeout )
# @Return: 1 or 0 
sub checkBackupNodeReplConfig {
    my ( $self, $backupHost, $masterHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
        
    # get replication user
    my $replUser = $self->{myconfObj}->get('dbUser');
    if ( !$replUser ) { 
        $log->error("$backupHost:$dbPort get replication user failed"); 
    }
    $log->debug("$backupHost:$dbPort get \$replUser is $replUser");

    my $checkReplConfig = 0;
    my $status  =  $self->getSlaveStatus($backupHost, $dbPort, $timeout);     
    if ( $status && %{$status} 
      && (exists $status->{master_host}) 
      && (exists $status->{master_port}) 
      && (exists $status->{master_user}) 
      && ($status->{master_host} eq $masterHost) 
      && ($status->{master_port} eq $dbPort)
      && ($status->{master_user} eq $replUser) ) 
     { 
        $checkReplConfig = 1;
        $log->info("$backupHost:$dbPort replication config is ok");
     }else{
        $log->error("$backupHost:$dbPort replication config is error");
     }

    return $checkReplConfig;
}

# @Description: check SlaveNode replication config
# @Param: ( $slaveHost,$masterHost,$backupHost,$dbPort,$timeout )
# @Return: 1 or 0 
sub checkSlaveNodeReplConfig {
    my ( $self,$slaveHost,$masterHost,$backupHost,$dbPort,$timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # get replication user
    my $replUser = $self->{myconfObj}->get('dbUser');
    if ( !$replUser ) { 
        $log->error("$slaveHost:$dbPort get replication user failed"); 
    }
    $log->debug("$slaveHost:$dbPort get \$replUser is $replUser");
    

    my $checkReplConfig = 0;
    my $status  =  $self->getSlaveStatus($slaveHost, $dbPort, $timeout);  
    if ( $status && %{$status} 
      && (exists $status->{master_host}) 
      && (exists $status->{master_port}) 
      && (exists $status->{master_user}) 
      && ($status->{master_host} eq $masterHost || $status->{master_host} eq $backupHost)
      && ($status->{master_port} eq $dbPort || $status->{master_port} eq $dbPort)
      && ($status->{master_user} eq $replUser) ) 
     { 
        $checkReplConfig = 1;
        $log->info("$slaveHost:$dbPort replication config is ok");
     } else {
        $log->error("$slaveHost:$dbPort replication config is error"); 
    }
    
    return $checkReplConfig;
}

# @Description: check replication thread
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: 3 or 2 or 1 or 4 or 0 
#   3: slave_io_running = No && slave_sql_running = No
#   2: slave_io_running = Yes && slave_sql_running = Yes
#   1: slave_io_running = No && slave_sql_running = Yes
#   4: slave_io_running = connecting && slave_sql_running = Yes
#   0: error
sub checkReplThread {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $checkReplThread = 0;
    my $slaveStatus  =  $self->getSlaveStatus($dbHost, $dbPort, $timeout);  
    if ( $slaveStatus && %{$slaveStatus} 
      && exists $slaveStatus->{slave_io_running} 
      && exists $slaveStatus->{slave_sql_running} ) {
        
        # case slave is oldmaster
        if ( (lc $slaveStatus->{slave_io_running} eq 'no') 
          && (lc $slaveStatus->{slave_sql_running} eq 'no') ) {
            $checkReplThread = 3;
            $log->info("$dbHost:$dbPort Slave_IO_Running is No, Slave_SQL_Running is No");
        
        # case skip init slave when keepalived start
        }elsif ( (lc $slaveStatus->{slave_io_running} eq 'yes') 
          && (lc $slaveStatus->{slave_sql_running} eq 'yes') ) {   
            $checkReplThread = 2;
            $log->info("$dbHost:$dbPort Slave_IO_Running is Yes, Slave_SQL_Running is Yes");
        
        # case could not connect to master
        } elsif ( (lc $slaveStatus->{slave_io_running} eq 'connecting') 
          && (lc $slaveStatus->{slave_sql_running} eq 'yes') ) {
            $checkReplThread = 4;
            $log->warn("$dbHost:$dbPort Slave_IO_Running is Connecting, Slave_SQL_Running is Yes");
            
        # case could not connect to master
        } elsif ( (lc $slaveStatus->{slave_io_running} eq 'no') 
          && (lc $slaveStatus->{slave_sql_running} eq 'yes') ) {
            $checkReplThread = 1;
            $log->warn("$dbHost:$dbPort Slave_IO_Running is No, Slave_SQL_Running is Yes");

        # replicate error
        } else { 
            $checkReplThread = 0;
            $log->error("$dbHost:$dbPort replication thread error");
            $log->error("Slave_IO_Running is $slaveStatus->{slave_io_running}");
            $log->error("Slave_SQL_Running is $slaveStatus->{slave_sql_running}");
        }
    } else {
        $checkReplThread = 0;
        $log->error("$dbHost:$dbPort get show slave status data failed");
    }
    
    return $checkReplThread;
}


# @Description: check replication data sync 
# @Param: $dbh $dbHost
# @Return: 1 or 0
sub checkReplSync {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    

    my $slaveStatus = $self->getSlaveStatus( $dbHost, $dbPort, $timeout );
    if ( !$slaveStatus ) {
        $log->error("$dbHost:$dbPort get show slave status data failed");
    }

    my $checkReplSync = 0;    
    if ( $slaveStatus && %{$slaveStatus} 
      && exists $slaveStatus->{master_log_file} 
      && exists $slaveStatus->{read_master_log_pos} 
      && exists $slaveStatus->{relay_master_log_file} 
      && exists $slaveStatus->{exec_master_log_pos} ) {            
        if ( $slaveStatus->{master_log_file} eq $slaveStatus->{relay_master_log_file} 
        && $slaveStatus->{read_master_log_pos} eq $slaveStatus->{exec_master_log_pos} ) {
            
        # case ok
            $checkReplSync = 1;
            $log->info("$dbHost:$dbPort have been catchup to master");
            $log->info("Master_Log_File: $slaveStatus->{master_log_file}");
            $log->info("Read_Master_Log_Pos: $slaveStatus->{read_master_log_pos}");
            $log->info("Relay_Master_Log_File: $slaveStatus->{relay_master_log_file}");
            $log->info("Exec_Master_Log_Pos: $slaveStatus->{exec_master_log_pos}");        
        # case bad    
        } else {
            $log->error("$dbHost:$dbPort doesn't catchup to master");
            $log->info("Master_Log_File: $slaveStatus->{master_log_file}");
            $log->info("Read_Master_Log_Pos: $slaveStatus->{read_master_log_pos}");
            $log->info("Relay_Master_Log_File: $slaveStatus->{relay_master_log_file}");
            $log->info("Exec_Master_Log_Pos: $slaveStatus->{exec_master_log_pos}");
        }
    }
    
    return $checkReplSync;
}


# @Description: Enable slave-side events and start event scheduling
# @Param ( $dbHost, $dbPort, $timeout )
# @Return 1 or undef
sub turnOnEventScheduler {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }

    my $timeoutMsg = "EON_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
        
    eval {    
        alarm $timeout;            
        # query and re-enable events on slave
        my $sql = "select EVENT_SCHEMA,EVENT_NAME"; 
        $sql .= "from information_schema.events"; 
        $sql .= "where status='SLAVESIDE_DISABLED'";
        my $slaveEvents = $dbh->selectall_arrayref($sql, {Slice => {}});
        foreach my $slaveEvent ( @$slaveEvents ) {
            my $sqlEnableEvent = "alter event " . $slaveEvent->{EVENT_SCHEMA} 
                  . "." . $slaveEvent->{EVENT_NAME} . " enable";
            my $rows = $dbh->do($slaveEvent);
        }
    
        # turn on the scheduler
        my $tSql = "SET GLOBAL event_scheduler=ON";
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0; 
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort turnOnEventScheduler timeout");
        }else{
            $log->error("$dbHost:$dbPort turnOnEventScheduler failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
           
        return;
    }
    
    $log->info("$dbHost:$dbPort turnOnEventScheduler success");    
        
    return 1; 
}


# @Description: log [show master status] data to mysql
# @Param: ( $dbHost, $dbPort, $state, $timeout )
# @Return: 1 or undef
sub logBinlogPosToDb {
    my ( $self, $dbHost, $dbPort, $state, $timeout ) = @_;    
    
    my $log = Log::Log4perl->get_logger(""); 
    
    my $status = $self->getMasterStatus($dbHost, $dbPort, 10);     
    if ( $status && %{$status} && exists $status->{file} && exists $status->{position} ) {
     
        my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
        if ( !$dbh ){
            $log->error("connect $dbHost:$dbPort failed");
            return;
        }
        
        my $logFile = $status->{file};
        my $logPos = $status->{position};           
        
        my $dbName = $self->{myconfObj}->get('dbName');
        if ( !$dbName ) { 
            $log->error("$dbHost:$dbPort get dbName failed"); 
        }
    
        my $tableName = $self->{myconfObj}->get('tableName'); # get tableName
        if ( !$tableName ) { 
            $log->error("$dbHost:$dbPort get tableName failed");
        }
        
        my $logTime = $self->getLocaltime();
        if ( !$logTime ) { 
            $log->error("$dbHost:$dbPort get \$logTime failed"); 
        }
        
        my $serverId = $self->getVariableValue($dbHost, $dbPort, 
                              "show variables like 'server_id'", 5);
        if (!$serverId){
            $log->error("$dbHost:$dbPort get serverId failed");
        }
        
        my $timeoutMsg = "CTb_TIMEOUT"; 
        my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
        my $oldaction = POSIX::SigAction->new();
        sigaction( SIGALRM, $action, $oldaction );
                            
        my $sql = "insert into " . $dbName . "." . $tableName; 
        $sql .= "(server_id, ip, log_file, log_pos, state, log_time) values (";
        $sql .= $serverId . ",'" . $dbHost .  "','" . $logFile . "'," . $logPos; 
        $sql .= ",'" . $state . "','" . $logTime . "')";
        
        eval {
            alarm $timeout; 
            $dbh->do("set session sql_log_bin=off");
            $dbh->do($sql);
            $dbh->do("set session sql_log_bin=on");
            $self->{mysqlConnObj}->disConnect($dbh);
            alarm 0;
        };
        alarm 0; # race condition protection
        sigaction( SIGALRM, $oldaction );  # restore original signal handler           
        if ( $@ ) {
            if ( $@ =~ /$timeoutMsg/ ) {
                $log->error("$dbHost:$dbPort write binlog pos to db timeout");
            }else{
                $log->error("$dbHost:$dbPort write binlog pos to db failed");
            }
            undef $@;
            if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                
            return;
        } 
        $log->info("$dbHost:$dbPort write binlog pos to db success.");
        $log->info("SQL:  $sql");         
        
        return 1;
    } else {
        return;
    }
}

# @Description: log master status to file
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub logBinlogPosToFile {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    my $log = Log::Log4perl->get_logger("");

    my $status = $self->getMasterStatus($dbHost, $dbPort, $timeout);   
    if ( $status && %{$status} && exists $status->{file} && exists $status->{position} ) {    
        my $logFile = $status->{file};
        my $logPos = $status->{position};
        my $msg = "file=$logFile, position=$logPos";
        $log->info("$dbHost:$dbPort binlog pos: $msg");  
              
        return 1;
    } else { 
     
        return; 
    }
}


# @Description: log slave status to file
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub logSlaveInfoToDb {
    my ( $self, $dbHost, $dbPort, $state, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $status = $self->getSlaveStatus($dbHost, $dbPort, $timeout);  
    if ( $status && %{$status} 
      && exists $status->{relay_master_log_file} 
      && exists $status->{exec_master_log_pos} )
    {    
        my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
        if ( !$dbh ){
            $log->error("connect $dbHost:$dbPort failed");
            return;
        }
        
        # file && pos
        my $logFile = $status->{relay_master_log_file};
        my $logPos = $status->{exec_master_log_pos};
        
        my $dbName = $self->{myconfObj}->get('dbName');
        if ( !$dbName ) { 
            $log->error("$dbHost:$dbPort get dbName failed"); 
        }
    
        my $tableName = $self->{myconfObj}->get('tableName'); # get tableName
        if ( !$tableName ) { 
            $log->error("$dbHost:$dbPort get tableName failed");
        }
        
        my $logTime = $self->getLocaltime();
        if ( !$logTime ) { 
            $log->error("$dbHost:$dbPort get \$logTime failed"); 
        }
        
        my $serverId = $self->getVariableValue($dbHost, $dbPort, 
                              "show variables like 'server_id'", 5);
        if (!$serverId){
            $log->error("$dbHost:$dbPort get serverId failed");
        }
        
        my $timeoutMsg = "CRTb_TIMEOUT"; 
        my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
        my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
        my $oldaction = POSIX::SigAction->new();
        sigaction( SIGALRM, $action, $oldaction );
                            
        my $sql = "insert into " . $dbName . "." . $tableName; 
        $sql .= "(server_id, ip, log_file, log_pos, state, log_time) values (";
        $sql .= $serverId . ",'" . $dbHost .  "','" . $logFile . "'," . $logPos; 
        $sql .= ",'" . $state . "','" . $logTime . "')";
        
        eval {
            alarm $timeout; 
            $dbh->do("set session sql_log_bin=off");
            $dbh->do($sql);
            $dbh->do("set session sql_log_bin=on");
            $self->{mysqlConnObj}->disConnect($dbh);
            alarm 0;
        };
        alarm 0; # race condition protection
        sigaction( SIGALRM, $oldaction );  # restore original signal handler           
        if ( $@ ) {
            if ( $@ =~ /$timeoutMsg/ ) {
                $log->error("$dbHost:$dbPort log self slave info to db timeout");
            }else{
                $log->error("$dbHost:$dbPort log self slave info to db failed");
            }
            undef $@;
            if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                
            return;
        } 
        $log->info("$dbHost:$dbPort log self slave info to db success.");
        $log->info("SQL:  $sql");
       
        return 1;
    } else { 
     
        return; 
    }
}


# @Description: log slave status to file
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub logSlaveInfoToFile {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $status = $self->getSlaveStatus($dbHost, $dbPort, $timeout);  
    if ( $status && %{$status} 
      && exists $status->{relay_master_log_file} 
      && exists $status->{exec_master_log_pos} )
    {    
        my $logFile = $status->{relay_master_log_file};
        my $logPos = $status->{exec_master_log_pos};
        
        my $msg = "relay_master_log_file=$logFile, exec_master_log_pos=$logPos";
        $log->info("$dbHost:$dbPort slave info: $msg");
       
        return 1;
    } else { 
     
        return; 
    }
}


# @Description: get newmaster binlog pos from database
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: $status or undef
sub getNewMasterBinlogPosFromDb {
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
        
    my $dbName = $self->{myconfObj}->get('dbName'); # get dbName
    if ( !$dbName ) { 
        $log->warn("$dbHost:$dbPort get dbName failed, set dbName=mysql_identity"); 
        $dbName = "mysql_identity";
    }
    
    my $tableName = $self->{myconfObj}->get('tableName'); # get tableName
    if ( !$tableName ) { 
        $log->warn("$dbHost:$dbPort get tableName failed, set tableName=rep_log_info");
        $tableName = "rep_log_info";
    }
    
    my $logIntervalTime = $self->{myconfObj}->get('logIntervalTime');
    if ( !$logIntervalTime ) { 
        $log->warn("$dbHost:$dbPort get logIntervalTime failed, set it 600s");
        $logIntervalTime = 600;
    }

    my $serverId = $self->getVariableValue($dbHost, $dbPort, 
                          "show variables like 'server_id'", 5);
    if (!$serverId){
        $log->error("$dbHost:$dbPort get serverId failed");
    }
                
    my $sql = "select server_id, log_file, log_pos, log_time, ip from ";
    $sql .= $dbName . "." . $tableName . " where server_id=" . $serverId;
    $sql .= " and state='MASTER' and unix_timestamp(log_time) > ";
    $sql .= " unix_timestamp(DATE_SUB(now(), INTERVAL ";
    $sql .= $logIntervalTime . " SECOND)) order by log_time desc limit 1";        
    $log->debug($sql);
        
    my $timeoutMsg = "NMBP_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );
       
    my @row;
    eval {
        alarm $timeout; 
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        @row = $sth->fetchrow_array();
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);  

        my $status = {};                 
        $status->{serverId} = $row[0];
        $status->{logFile} = $row[1];
        $status->{logPos} = $row[2];
        $status->{logTime} = $row[3];
        $status->{ip} = $row[4];                
        if ( $status && %{$status} 
          && exists $status->{serverId} 
          && exists $status->{logFile} 
          && exists $status->{logPos} ) 
        {
            $log->info("$dbHost:$dbPort get New Master binlog pos from $dbName.$tableName: ");
            $log->info("File=$status->{logFile},  Pos=$status->{logPos}");
            
            return $status;
            
        } else {
            $log->error("$dbHost:$dbPort get newMaster binlog pos from $dbName.$tableName failed");
            $log->error("SQL:  $sql"); 
            
            return;        
        }    
        alarm 0;
    }; 
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler
   
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort get newMaster binlog pos from db timeout");
        }else{
            $log->error("$dbHost:$dbPort get newMaster binlog pos from db failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); }             
        
        return;
    }
}


# @Description: get show variables value 
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: value or undef
sub getVariableValue {
    my ( $self, $dbHost, $dbPort, $sql, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
          
    my $value;
    my $timeoutMsg = "VAL_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    eval {
        alarm $timeout;  
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        my @row = $sth->fetchrow_array();
        $value = $row[1];
        $sth->finish();
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0;  
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler    
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort get variable value timeout");
        }else{
            $log->error("$dbHost:$dbPort get variable value failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); }  
                    
        return;
    }
    
    $log->info("$dbHost:$dbPort get variable value success");    
    
    return $value; 
}

# @Description: check database exist
# @Param: ( $dbName, $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub checkDbExist {
    my ( $self, $dbName, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
    
    my $timeoutMsg = "CDE_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
        
    my $sql = "SHOW DATABASES LIKE '" . $dbName . "'";
    my $row;
    eval {
        alarm $timeout;           
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $row = $sth->fetchrow_array();
        $sth->finish(); 
        $self->{mysqlConnObj}->disConnect($dbh);  
        
        if ( !$row ) {
            $log->error("$dbHost:$dbPort found $dbName doesn't exist");
            return;
        }     
 
        alarm 0;   
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort execute $sql timeout");
        }else{
            $log->error("$dbHost:$dbPort execute $sql failed");
        }    
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                
        return;
    }
  
    $log->info("$dbHost:$dbPort found $dbName exist");
    
    return 1;
}

# @Description: check table exist 
# @Param: ( $dbName, $tableName, $dbPort, $timeout )
# @Return: 1 or undef
sub checkTableExist {
    my ( $self, $dbName, $tableName, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }

    my $timeoutMsg = "CTE_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
            
    my $sql = "show create table ". $dbName .".". $tableName;
    my $row;
    eval {
        alarm $timeout;           
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        $row = $sth->fetchrow_array();
        $sth->finish();     
        $self->{mysqlConnObj}->disConnect($dbh);
        if ( !$row ) {
            $log->error("$dbHost:$dbPort found $tableName doesn't exist");
            return;
        }  
        alarm 0;   
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort execute $sql timeout");
        }else{
            $log->error("$dbHost:$dbPort execute $sql failed");
        }    
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                
        return;
    }     
    $log->info("$dbHost:$dbPort found $tableName exist");     
    
    return 1;
}
  
# @Description: create database 
# @Param: ( $dbName, $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub createDb {
    my ( $self, $dbName, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
        
    my $timeoutMsg = "CDb_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );
         
    my $sql = "CREATE DATABASE IF NOT EXISTS " . $dbName;      
    eval {
        alarm $timeout;   
        $dbh->do("SET session sql_log_bin=OFF");
        $dbh->do($sql);
        $dbh->do("SET session sql_log_bin=ON");
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0;
    };    
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler      

    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort create database $dbName timeout");
        }else{
            $log->error("$dbHost:$dbPort create database $dbName failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                    
        return;
    }
    
    $log->info("$dbHost:$dbPort create database $dbName success");    
    
    return 1;  
}

#
# @Description: create table 
# @Param:( $dbName, $tableName, $dbHost, $dbPort, $timeout )
# @Return: 1 or undef
sub createTable {
    my ( $self, $dbName, $tableName, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort, dbName => $dbName );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }

    my $timeoutMsg = "CTb_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );

    my $sql = "CREATE TABLE IF NOT EXISTS " . $tableName 
    . " ( server_id bigint(20) unsigned NOT NULL,
        log_file varchar(20) NOT NULL,
        log_pos int(10) unsigned NOT NULL,
        state varchar(10) DEFAULT NULL,
        log_time datetime DEFAULT NULL,
        ip varchar(20) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8";
    $log->debug("SQL=$sql"); # debug mode
            
    eval {
        alarm $timeout; 
        $dbh->do("SET session sql_log_bin=OFF");
        $dbh->do($sql);
        $dbh->do("SET session sql_log_bin=ON");
        $self->{mysqlConnObj}->disConnect($dbh);
        alarm 0;
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler     
    
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort create table $tableName timeout");
        }else{
            $log->error("$dbHost:$dbPort create table $tableName failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                    
        return;
    }
    
    $log->info("$dbHost:$dbPort create table $tableName success");    
                
    return 1;  
}


# @Description: kill mysql connection
# @Param: ( $dbHost, $dbPort, $timeout )
# @Return: $killCount or undef
sub killMysqlSession{
    my ( $self, $dbHost, $dbPort, $timeout ) = @_;
    
    my $log = Log::Log4perl->get_logger(""); 

    my $dbh = $self->{mysqlConnObj}->dbConnect( dbHost => $dbHost, dbPort => $dbPort );
    if ( !$dbh ){
        $log->error("connect $dbHost:$dbPort failed");
        return;
    }
        
    my $timeoutMsg = "KMS_TIMEOUT"; 
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 
    
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
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
        alarm 0; 
    };
    alarm 0; # race condition protection
    sigaction( SIGALRM, $oldaction );  # restore original signal handler
    
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("$dbHost:$dbPort kill session timeout");
        }else{
            $log->error("$dbHost:$dbPort kill session failed");
        }
        undef $@;
        if ($dbh){ $self->{mysqlConnObj}->disConnect($dbh); } 
                    
        return;
    }

    if ($killCount >0){
        $log->info("$dbHost:$dbPort kill session success");
    }else{
        $log->info("$dbHost:$dbPort has no session to kill");
    }    
                 
    return $killCount;
}


sub getLocaltime {
    my ( $self ) = @_;
    
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time());
    $year = $year + 1900;
    $mon++; 
    my $localtime = $year . "-" . $mon . "-" . $mday . " " . $hour . ":" . $min . ":" . $sec;
    
    return $localtime;
}

# perl包固定用法，"1"为true，告诉perl解释器包正确
1;

