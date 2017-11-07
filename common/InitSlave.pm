# Description:  init slave
# Authors:  
#   zhaoyunbo

package InitSlave;

use strict;
use warnings;
use Log::Log4perl;
use Parallel::ForkManager;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/../etc";
use Myconfig;
use CheckWvip;
use MysqlConn;
use MysqlManager;
use OpStatus;

# 字符串常量含义
use constant INIT_ERROR => "initSlave error null";       # 初始化slave失败
use constant INIT_SUCCESS => "initSlave success null";    # 初始化slave成功
use constant SKIP_NO_NEED => "initSlave skip master_is_already_newmaster";   # slave已经指向新主库，不初始化
use constant SKIP_DBCONN_ERROR => "initSlave skip dbconnect_error";   # 无法连接mysql,不初始化
use constant SKIP_NOT_SLAVE => "initSlave skip not_slave";    # 节点不是slave,不初始化
use constant SKIP_NOT_GROUP => "initSlave skip not_same_group";   # 节点不在同一组高可用中,不初始化
use constant SKIP_THREAD_ERROR => "initSlave skip replication_thread_error";   # 节点复制线程错误,不初始化
use constant SKIP_SLAVE_DATA_LAG => "initSlave skip slave_data_lag";    # 节点复制数据延迟,不初始化

# 返回值常量含义
use constant INIT_ERROR_CODE => 0;            # 初始化slave失败
use constant INIT_SUCCESS_CODE => 1;          # 初始化slave成功
use constant SKIP_NO_NEED_CODE => 2;          # slave已经指向新主库，不初始化
use constant SKIP_DBCONN_ERROR_CODE => 3;     # 无法连接mysql，不初始化
use constant SKIP_NOT_SLAVE_CODE => 4;        # 节点不是slave，不初始化
use constant SKIP_NOT_GROUP_CODE => 5;        # 节点不在同一组高可用中，不初始化
use constant SKIP_THREAD_ERROR_CODE => 6;     # 节点复制线程错误，不初始化
use constant SKIP_SLAVE_DATA_LAG_CODE => 7;   # 节点复制数据延迟，不初始化


sub new {
    my ( $class, %args ) = @_;
    
    my $self = {};  # create a hash ref
    
    my $log = Log::Log4perl->get_logger(""); 
    
    bless ( $self, $class ); 
    
    return $self;  # return an object 
}


# @Description: init slave
# @Param: ($vrrpInstName,$masterStatus,$slaves,
#         $oldMasterHost,$newMasterHost,$dbPort)
# @Return: 1 or 0
#          1: success  0: failed 
sub initSlave {
    my ($self,$vrrpInstName,$masterStatus,$slaves,
              $oldMasterHost,$newMasterHost,$dbPort) = @_;

    my $log = Log::Log4perl->get_logger("");  

    $log->info();
    $log->info();
    $log->info("[instance:$dbPort]:   start doing init slaves");
     
    # 使用Parallel::ForkManager模块并行初始化slave
    my $maxProcs = 10;
    my $pm = Parallel::ForkManager->new($maxProcs);
    
    $log->info("[instance:$dbPort]:   doing init slaves parallel...");  
    $log->info("[instance:$dbPort]:   use Parallel::ForkManager, maxProcs: $maxProcs"); 
        
    # setup a callback when a child finishes up, so we can get it's exit code
    $pm->run_on_finish(  sub { 
      my ( $pid, $exitCode, $target ) = @_;
      $log->info();
      $log->info("[instance:$dbPort]: $target finish, exitCode: $exitCode, pid: $pid");
      if ( $exitCode == 0 ){
          $log->error("[instance:$dbPort]: $target:$dbPort error initSlave");           
      }elsif( $exitCode == 1 ){
          $log->info("[instance:$dbPort]: $target:$dbPort success initSlave");             
      }elsif( $exitCode == 2 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, master_is_already_newmaster"); 
      }elsif( $exitCode == 3 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, dbconnect_error");            
      }elsif( $exitCode == 4 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, not_slave");            
      }elsif( $exitCode == 5 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, not_same_group");            
      }elsif( $exitCode == 6 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, replication_thread_error");     
      }elsif( $exitCode == 7 ){
          $log->info("[instance:$dbPort]: $target:$dbPort skip initSlave, slave_data_lag");
      }
    });
    
    $pm->run_on_start(
        sub { 
            my ($pid, $target)=@_;
            $log->info();
            $log->info("[instance:$dbPort]: $target start, pid: $pid");
        }
    );
    
    foreach my $target (@{$slaves}) {
        my $pid = $pm->start($target) and next;
        
        eval {
            my $exitCode = $self->slaveChangeMaster($vrrpInstName,$masterStatus,
                                 $target,$oldMasterHost,$newMasterHost,$dbPort);
            if ( $exitCode == 0 ){
                $pm->finish(0);
            } elsif ( $exitCode == 1 ){
                $pm->finish(1);
            } elsif ( $exitCode == 2 ){
                $pm->finish(2);
            } elsif ( $exitCode == 3 ){
                $pm->finish(3);
            } elsif ( $exitCode == 4 ){
                $pm->finish(4);
            } elsif ( $exitCode == 5 ){
                $pm->finish(5);
            } elsif ( $exitCode == 6 ){
                $pm->finish(6);
            } elsif ( $exitCode == 7 ){
                $pm->finish(7);
            } else {
                $pm->finish(0);    
            }
        };
        if ($@) {
            $log->error("error in slaveChangeMaster, $@");
            undef $@;
            $pm->finish(0);
        }
    }

    $pm->wait_all_children;
}


# @Description: slaveChangeMaster
# @Param: $vrrpInstName,$masterStatus,$target,
#         $oldMasterHost,$newMasterHost,$dbPort )
# @Return: 0~7
sub slaveChangeMaster {
    my ( $self,$vrrpInstName,$masterStatus,$target,
         $oldMasterHost,$newMasterHost,$dbPort ) = @_;  

    my $log = Log::Log4perl->get_logger("");
    
    # 初始化slave结果标识   
    my $exitCode = INIT_ERROR_CODE;
    
    # 待初始化节点角色  
    my $dbRole;
    if ( $target ne $oldMasterHost ) {
        $dbRole = "slave";
    }else{
        $dbRole = "oldmaster";             
    }
    
    # 初始化slave状态文件
    my $opStatus = $vrrpInstName." ".$target." ".$dbPort." ".$dbRole; 
    my $op = "";
         
    # create object
    my $myconfObj = new Myconfig();
    my $mysqlConnObj = new MysqlConn( myconfObj => $myconfObj );
    my $mmObj = new MysqlManager( 
                    myconfObj => $myconfObj,
                    mysqlConnObj => $mysqlConnObj
                );
    
    # get retry num and times                
    my $initSlaveRetryNum = $myconfObj->get('initSlaveRetryNum');
    if ( !$initSlaveRetryNum ) { 
        $log->error("get initSlaveRetryNum failed, set it 5 times");
        $initSlaveRetryNum = 5;
    }

    my $waitRetryTime = $myconfObj->get('initSlaveWaitRetryTime');
    if ( !$waitRetryTime ) { 
        $log->error("get waitRetryTime failed, set it 5 times");
        $waitRetryTime = 10;
    }
        
    $log->info();
    $log->info("[instance:$dbPort]: $target:$dbPort start doing init slave");  
    
    # 初始化slave失败重试机制     
    for ( my $i=0; $i<$initSlaveRetryNum; $i++ ) {
        $log->info("[instance:$dbPort]: $target:$dbPort do the $i time");   
            
        #  step1 检查mysql连接
        $log->info();
        $log->info("[instance:$dbPort]: step1 $target:$dbPort start dbConnect"); 
        
        # 连接mysql
        my $dbh = $mysqlConnObj->dbConnect( dbHost => $target, dbPort => $dbPort );
        if ( !$dbh ){      
            # 最后一次重连mysql失败
            if ( $i == ($initSlaveRetryNum-1) ){
                # target connect db failed, skip initSlave             
                $log->error("[instance:$dbPort]: step1 $target:$dbPort connect db failed");                 
                $log->error("[instance:$dbPort]: step1 $target:$dbPort skip initSlave");
                
                $exitCode = SKIP_DBCONN_ERROR_CODE;       
                $op = SKIP_DBCONN_ERROR;
                $opStatus .= " ". $op . " \n";
                opStatus($myconfObj, $opStatus, "append");
                
                # 结束循环
                last;
            }
            $log->error("[instance:$dbPort]: step1 $target:$dbPort dbConnect failed");
            $log->info("[instance:$dbPort]: step1 $target:$dbPort sleep $waitRetryTime s");            
            
            # 重试时间间隔
            sleep $waitRetryTime;
            $log->info("[instance:$dbPort]: step1 $target:$dbPort retry dbConnect");
            # 继续重试
            next;
        }
        # 连接mysql成功
        $log->info("[instance:$dbPort]: step1 $target:$dbPort success");
                
        # 检查目标节点能否做initSlave
        $log->info();
        $log->info("[instance:$dbPort]:  $target:$dbPort start checking replication info");
        # 若目标节点是旧主库
        if ( $target eq $oldMasterHost ){ 
            $log->info("[instance:$dbPort]: step2 $target:$dbPort is oldmaster");             
            
            # step2 checkReplThread
            $log->info();       
            $log->info("[instance:$dbPort]: step2 $target:$dbPort checking repl thread");
                        
            my $checkReplThread = $mmObj->checkReplThread($target,$dbPort,5);
            # checkReplThread函数返回值： 0|1|2|3|4
            # $checkReplThread值含义如下：
            # 3: 表示复制线程为slave_io_running = No和slave_sql_running = No
            # 2: 表示复制线程为slave_io_running = Yes和slave_sql_running = Yes
            # 1: 表示复制线程为slave_io_running = No和slave_sql_running = Yes
            # 4: 表示复制线程为slave_io_running = connecting和slave_sql_running = Yes
            # 0: error    
                        
            # 目标节点复制线程正常
            # checkReplThread=2
            if($checkReplThread == 2){
            	# get repl user
            	my $replUser = $myconfObj->get('dbUser');
            	if ( !$replUser ) { 
            		$log->error("$target:$dbPort get repl user failed");
            	}
                
                my $slaveStatus = $mmObj->getSlaveStatus($target,$dbPort,5);            	
                if ( $slaveStatus && %{$slaveStatus} 
                  && exists $slaveStatus->{master_host} 
                  && exists $slaveStatus->{master_port} 
                  && exists $slaveStatus->{master_user} 
                  && $slaveStatus->{master_host} eq $newMasterHost 
                  && $slaveStatus->{master_port} eq $dbPort 
                  && $slaveStatus->{master_user} eq $replUser ) 
                {  
                   # 目标节点repl master已经指向newMasterHost,不需要初始化
                   # 对应场景是初始化环节，主库keepalived第一次启动  
                   # target replication is in right state, skip initSlave
                   $exitCode = SKIP_NO_NEED_CODE; 
                   
                   $op = SKIP_NO_NEED;
                   $opStatus .= " ". $op . " \n";
                   opStatus($myconfObj, $opStatus, "append");
                   
                   $log->info("[instance:$dbPort]: step2 $target:$dbPort replication thread ok");                      
                   $log->info("[instance:$dbPort]: step2 $target:$dbPort repl master is already newmaster");
                   $log->info("[instance:$dbPort]: step2 $target:$dbPort skip initSlave");
                   
                   # 结束循环                  
                   last;
               }
            }
        }else{
        ## begin 若目标节点不是旧主       
            $log->info("[instance:$dbPort]: $target:$dbPort is not oldmaster");

            # step2 检查目标节点是否为从库
            $log->info();
            $log->info("[instance:$dbPort]: step2 $target:$dbPort checking is slave or not");
            
            my $slaveStatus = $mmObj->getSlaveStatus($target,$dbPort,5);
            
            if (!$slaveStatus){
                # 目标节点show slave status为空, 该节点不是正常从库 
                # 不初始化该节点
                $exitCode = SKIP_NOT_SLAVE_CODE;                
                $log->error("[instance:$dbPort]: step2 $target:$dbPort is not a slave");                 
                $log->error("[instance:$dbPort]: step2 $target:$dbPort skip initSlave");
                
                $op = SKIP_NOT_SLAVE;
                $opStatus .= " ". $op . " \n";
                opStatus($myconfObj, $opStatus, "append");
                
                # 结束循环
                last;
            }
            # 目标节点show slave status有数据
            $log->info("[instance:$dbPort]: step2 $target:$dbPort success");  
            
            
            # step3 检查目标节点复制配置、复制关系
            $log->info();
            $log->info("[instance:$dbPort]: step3 $target:$dbPort checking repl config");
             
            my $checkReplConf = $mmObj->checkSlaveNodeReplConfig( $target,
                                         $newMasterHost,$oldMasterHost,$dbPort,5);
                                              
            if (!$checkReplConf){
                # 目标节点不在当前高可用组中, 不初始化该节点
                $exitCode = SKIP_NOT_GROUP_CODE;
                                
                $op = SKIP_NOT_GROUP;
                $opStatus .= " ". $op . " \n";
                opStatus($myconfObj, $opStatus, "append");

                $log->error("[instance:$dbPort]: step3 $target:$dbPort not in same group");         
                $log->error("[instance:$dbPort]: step3 $target:$dbPort skip initSlave");
                
                # 结束循环
                last;
            }
            # 目标节点复制配置、复制关系正常
            $log->info("[instance:$dbPort]: step3 $target:$dbPort success");  
            
            
            # step4 检查目标节点复制线程
            $log->info();        
            $log->error("[instance:$dbPort]: step4 $target:$dbPort checking repl thread"); 
            
            my $checkReplThread = $mmObj->checkReplThread($target,$dbPort,5);
            # checkReplThread函数返回值： 0|1|2|3|4
            # $checkReplThread值含义如下：
            # 3: 表示复制线程为slave_io_running = No和slave_sql_running = No
            # 2: 表示复制线程为slave_io_running = Yes和slave_sql_running = Yes
            # 1: 表示复制线程为slave_io_running = No和slave_sql_running = Yes
            # 4: 表示复制线程为slave_io_running = connecting和slave_sql_running = Yes
            # 0: error          
            
            # 根据上述$checkReplThread值检查判断目标节点复制线程
            
            # 目标节点复制线程error($checkReplThread=0)，不初始化该节点
            if (!$checkReplThread){
                # target replication thread error, skip initSlave
                $exitCode = SKIP_THREAD_ERROR_CODE;
                           
                $op = SKIP_THREAD_ERROR;
                $opStatus .= " ". $op . " \n";
                opStatus($myconfObj, $opStatus, "append");
                
                $log->error("[instance:$dbPort]: step4 $target:$dbPort repl thread error");
                $log->error("[instance:$dbPort]: step4 $target:$dbPort skip initSlave");
                
                # 结束循环
                last;
            
            # 目标节点复制线程正常($checkReplThread=2)
            }elsif($checkReplThread == 2){
            	# get repl user
            	my $replUser = $myconfObj->get('dbUser');
            	if ( !$replUser ) { 
            		$log->error("$target:$dbPort get repl user failed");
            	}
            	
                if ( $slaveStatus && %{$slaveStatus} 
                  && exists $slaveStatus->{master_host} 
                  && exists $slaveStatus->{master_port} 
                  && exists $slaveStatus->{master_user} 
                  && $slaveStatus->{master_host} eq $newMasterHost 
                  && $slaveStatus->{master_port} eq $dbPort 
                  && $slaveStatus->{master_user} eq $replUser ) 
                {  
                   # 目标节点repl master已经指向newMasterHost,不需要初始化
                   # 对应场景是初始化环节，主库keepalived第一次启动 
                   $exitCode = SKIP_NO_NEED_CODE; 
                   
                   $op = SKIP_NO_NEED;
                   $opStatus .= " ". $op . " \n";
                   opStatus($myconfObj, $opStatus, "append");
                   
                   $log->info("[instance:$dbPort]: step4 $target:$dbPort replication thread ok");                      
                   $log->info("[instance:$dbPort]: step4 $target:$dbPort repl master is already newmaster");
                   $log->info("[instance:$dbPort]: step4 $target:$dbPort skip initSlave"); 
                   
                   # 结束循环                 
                   last;
                }
             #  目标节点复制线程异常($checkReplThread=3),不做初始化
             }elsif($checkReplThread == 3){
                 
                 # target replication thread error, skip initSlave
                 $exitCode = SKIP_THREAD_ERROR_CODE;  
                 
                 $op = SKIP_THREAD_ERROR;
                 $opStatus .= " ". $op . " \n";
                 opStatus($myconfObj, $opStatus, "append");
                 
                 $log->error("[instance:$dbPort]: step4 $target:$dbPort replication thread error");           
                 $log->error("[instance:$dbPort]: step4 $target:$dbPort skip initSlave");
                 
                 # 结束循环                  
                 last;
             }

             # 目标节点复制线程正常
             $log->info("[instance:$dbPort]: step4 $target:$dbPort success");
                          
             # step5 检查目标节点数据同步
             $log->info();
             $log->info("[instance:$dbPort]: step5 $target:$dbPort checking repl data sync");  
             
             my $syncRetryNum = $myconfObj->get('checkReplSyncRetryNum');
             if ( !$syncRetryNum ) {
                 $syncRetryNum = 3;
                 $log->warn("$target:$dbPort get syncRetryNum failed, set it 3");
             }
             
             my $syncWaitRetryTime = $myconfObj->get('checkReplSyncWaitRetryTime'); 
             if ( !$syncWaitRetryTime ) { 
                 $syncWaitRetryTime = 3;
                 $log->warn("$target:$dbPort get syncWaitRetryTime failed, set it 3s");
             }
    
             # 检查目标节点数据同步重试机制   
             my $replSyncOk = 0;             
             for ( my $j=0; $j<$syncRetryNum; $j++ ) {
                 $log->info("[instance:$dbPort]: step5 $target:$dbPort do the $j time");  
                 my $checkReplSync = $mmObj->checkReplSync($target,$dbPort,10);
                 if (!$checkReplSync){
                     # 最后一次检查数据还存在延迟，结束循环
                     if ( $j == ($syncRetryNum-1) ){
                         last;
                     }
                     $log->info();
                     $log->info("[instance:$dbPort]: step5 $target:$dbPort data doesn't catchup master");
                     $log->info("[instance:$dbPort]: step5 $target:$dbPort sleep $syncWaitRetryTime s"); 
                   
                     sleep $syncWaitRetryTime;
                     
                     # 继续重试检查
                     $log->info("[instance:$dbPort]: step5 $target:$dbPort retry checking data sync");
                     next;
                
                 } else {
                     # 复制数据已经应用完成，结束循环
                     $log->info();
                     $log->info("[instance:$dbPort]: step5 $target:$dbPort data catchup master"); 
                     $log->info("[instance:$dbPort]: step5 $target:$dbPort data sync is OK"); 
                     $replSyncOk = 1;
                     last; 
                 }
             }
             if ( !$replSyncOk ){
                 # 目标节点复制数据存在延迟, 不做初始化
                 $exitCode = SKIP_SLAVE_DATA_LAG_CODE;
                 
                 $op = SKIP_SLAVE_DATA_LAG;
                 $opStatus .= " ". $op . " \n";
                 opStatus($myconfObj, $opStatus, "append");
    
                 $log->error("[instance:$dbPort]: step5 $target:$dbPort data doesn't catchup master"); 
                 $log->error("[instance:$dbPort]: step5 $target:$dbPort repl data Lag"); 
                 $log->error("[instance:$dbPort]: step5 $target:$dbPort skip initSlave"); 
                 last;
             }
             # 目标节点复制数据已经应用完成
             $log->info("[instance:$dbPort]: step5 $target:$dbPort success");                        
             
            
             # step6 目标节点stop slave
             $log->info();
             $log->info("[instance:$dbPort]: step6 $target:$dbPort start stopping slave");
              
             my $stopSlave = $mmObj->executeSql($target, $dbPort, "stop slave", 10);
             if (!$stopSlave){
                 $log->error("[instance:$dbPort]: step6 $target:$dbPort failed");
             }else{
                 $log->info("[instance:$dbPort]: step6 $target:$dbPort success"); 
             }
             
             # step7 目标节点记录slave info到文件
             $log->info();
             $log->info("[instance:$dbPort]: step7 $target:$dbPort start logSlaveInfoToFile"); 
                      
             my $logSlaveInfoToFile = $mmObj->logSlaveInfoToFile($target, 
                                                               $dbPort, 10);
             if ( !$logSlaveInfoToFile ) {
                 $log->error("[instance:$dbPort]: step7 $target:$dbPort failed");
             }else{
                 $log->info("[instance:$dbPort]: step7 $target:$dbPort success");
             }             
         } ## end 若目标节点不是旧主

         
         ## 目标节点不分旧主和其它从库，都需要以下一系列操作
         
         # step8 记录目标节点binlog同步点到文件
         $log->info();            
         $log->info("[instance:$dbPort]: step8 $target:$dbPort start logBinlogPosToFile"); 
       
         my $logBinlogPosToFile = $mmObj->logBinlogPosToFile($target,
                                                         $dbPort, 10);
         if ( !$logBinlogPosToFile ) {
             $log->error("[instance:$dbPort]: step8 $target:$dbPort failed");
         }else{
             $log->info("[instance:$dbPort]: step8 $target:$dbPort success");
         }    
                                    
         # step9 获取主库binlog同步点
         $log->info();
         $log->info("[instance:$dbPort]: step9 $target:$dbPort start getting newmaster binlog pos");
           
         my ( $masterLogFile,$masterLogPos,$getMode );
         
         if ( $masterStatus && %{$masterStatus} 
           && exists $masterStatus->{file} 
           && exists $masterStatus->{position} ) 
         {
             # 从哈希变量$masterStatus获取新主库binlog同步点
             $masterLogFile = $masterStatus->{file};
             $masterLogPos = $masterStatus->{position};
             $getMode = "HASH";
         } else {      
             # 从rep_log_info表获取新主库binlog同步点
             my $masterStatus = $self->getNewMasterBinlogPosFromDb($newMasterHost,
                                                                     $dbPort, 10);
             if ( $masterStatus && %{$masterStatus} 
               && exists $masterStatus->{logFile} 
               && exists $masterStatus->{logPos} ) 
             {
                 $masterLogFile = $masterStatus->{logFile};
                 $masterLogPos = $masterStatus->{logPos};
                 $getMode = "DB";
             } else {
                 $log->error("[instance:$dbPort]: step9 $target:$dbPort failed");
             }
         }
         $log->info("[instance:$dbPort]: step9 $target:$dbPort get newMaster pos from $getMode");
         my $binlogPosLog = "########## $target:$dbPort newMaster binlog pos:  ";
         $binlogPosLog .= "Master_Log_File: $masterLogFile, Master_Log_Pos: $masterLogPos";
         $log->info();
         $log->info("  ===========   New Master Binlog Pos:  ==========    ");         
         $log->info("$binlogPosLog");
         $log->info();
         $log->info();
         
         if ( !$masterLogFile || !$masterLogPos ){
             $log->error("[instance:$dbPort]: step9 $target:$dbPort failed");
             $log->error("[instance:$dbPort]: step9 $target:$dbPort stop at step9");
             $log->info("[instance:$dbPort]: step9 $target:$dbPort retry slaveChangeMaster");
             next;
         }
         $log->info("[instance:$dbPort]: step9 $target:$dbPort success");
         
         
         # step10 调整目标节点复制指向新主库
         $log->info();
         $log->info("[instance:$dbPort]: step10 $target:$dbPort start changeMasterTo"); 
         
         my $changeMasterTo = $mmObj->changeMasterTo($target,$newMasterHost,
                                       $dbPort,$masterLogFile,$masterLogPos,30);                                                  
         if (!$changeMasterTo){
             $log->error("[instance:$dbPort]: step10 $target:$dbPort failed");
             $log->error("[instance:$dbPort]: step10 $target:$dbPort stop at step10");
             $log->info("[instance:$dbPort]: step10 $target:$dbPort retry slaveChangeMaster");
             next;
         }else{
             $log->info("[instance:$dbPort]: step10 $target:$dbPort success");
         }
                       
         # step11 目标节点start slave
         $log->info();
         $log->info("[instance:$dbPort]: step11 $target:$dbPort starting slave"); 
         
         my $startSlave = $mmObj->executeSql($target,$dbPort,"start slave",10);
         if (!$startSlave){
             $log->error("[instance:$dbPort]: step11 $target:$dbPort failed");             
         }else{
             $log->info("[instance:$dbPort]: step11 $target:$dbPort success");         
         }
          
         # step12 目标节点设置为只读
         $log->info();       
         $log->info("[instance:$dbPort]: step12 $target:$dbPort start setting read_only=on"); 
         
         my $setMysqlReadonly = $mmObj->executeSql($target,$dbPort,
                                                 "SET GLOBAL READ_ONLY=ON",10);
         if (!$setMysqlReadonly){
             $log->error("[instance:$dbPort]: step12 $target:$dbPort failed");
         }else{
             $log->info("[instance:$dbPort]: step12 $target:$dbPort success");
         }
          
         # step13 旧主关闭event scheduler,其它从库不需要关闭
         $log->info();     
         $log->info("[instance:$dbPort]: step13 $target:$dbPort start stopping event scheduler"); 
         if ( $target eq $oldMasterHost ) {
             my $stopEvent = $mmObj->executeSql($target,$dbPort,
                                          "SET GLOBAL event_scheduler=OFF",10);
             if (!$stopEvent){
                 $log->error("[instance:$dbPort]: step13 $target:$dbPort failed");
             }else{
                 $log->info("[instance:$dbPort]: step13 $target:$dbPort success");
             }
         }
         
         # 初始化目标节点为从库成功，设置成功标志  
         $exitCode = INIT_SUCCESS_CODE;
         
         # 打印初始化结果
         $op = INIT_SUCCESS;
         $opStatus .= " ". $op . " \n";
         opStatus($myconfObj, $opStatus, "append");
             
         # 关闭mysql连接
         if ($dbh){
             $mysqlConnObj->disConnect($dbh);
         }
         
         # 目标节点初始化成功，结束重试循环
         $log->info();
         $log->info("[instance:$dbPort]: $target:$dbPort finish initSlave");
         last;    
    }
      
    return $exitCode;  
}

1;

