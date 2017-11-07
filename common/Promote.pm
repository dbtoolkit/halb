# Description:  promote mysql to be master
# Authors:  
#   zhaoyunbo

package Promote;

use strict;
use warnings;
use Log::Log4perl;
use POSIX qw(:signal_h);

use FindBin qw($Bin);
use lib "$Bin/../etc";

use Myconfig;
use MysqlConn;
use MysqlManager;
use CheckWvip;
use ManagerUtil;
use OpStatus;
use InitSlave;

# 字符串常量含义
use constant PROMOTE_ERROR => "promote error null";  # 提升失败
use constant PROMOTE_SUCCESS => "promote success null";   # 提升成功
use constant MASTER_SKIP_PROMOTE => "promote skip it_is_current_master"; # master不提升
use constant REPL_CONFIG_ERROR_SKIP_PROMOTE => "promote skip repl_config_error";  # 备主复制配置错误
use constant REPL_THREAD_ERROR_SKIP_PROMOTE => "promote skip repl_thread_error";  # 备主复制线程错误
use constant REPL_DATA_LAG_SKIP_PROMOTE => "promote skip repl_data_lag";   # 备主复制数据延迟
use constant WRITEVIP_ERROR_SKIP_PROMOTE => "promote skip writevip_error"; # 写vip不可用

# @Description: constructor
# @Param: 
# @Return: $self 
sub new {
    my ( $class, %args ) = @_;
    my $self = {};  # create a hash ref   

    bless ( $self, $class );  # statement object type
    return $self;  # return an object 
}


# @Description: 提升mysql备主为主库 
# @Param: ($vrrpInstName,$slaves,$oldMasterHost,$newMasterHost,$dbPort)
# @Return: 1:success  2:ignore  0:failed
sub main {
    my ($self,$vrrpInstName,$slaves,$oldMasterHost,$newMasterHost,$dbPort) = @_;
     
    # 提升结果标识
    my $exitCode = 0;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 创建对象
    my $myconfObj = new Myconfig();
    my $mysqlConnObj = new MysqlConn( myconfObj => $myconfObj );
    my $mmObj = new MysqlManager( 
                    myconfObj => $myconfObj,
                    mysqlConnObj => $mysqlConnObj 
                );

    # 用于打印提升结果
    my $opStatus = $vrrpInstName." ".$newMasterHost." ".$dbPort; 
    my $op = "";
                
    # 检查脑裂，若给定时间内ping不通网关，则不提升
    $log->info();
    $log->info("[instance:$dbPort] $newMasterHost:$dbPort checking writeVip available");  
    my $checkWriteVip = checkWvip($vrrpInstName);
    if (defined($checkWriteVip) && $checkWriteVip == 1){
        # 写vip可用，且ping通网关
        $log->info("[instance:$dbPort] $newMasterHost:$dbPort writeVip is ok");     
        
        # 1. 检查当前节点是否为备主
        $log->info();
        $log->info("[instance:$dbPort] step1: $newMasterHost:$dbPort start checking mysql role");
        my $slaveStatusData = $mmObj->getSlaveStatus($newMasterHost,$dbPort,10);
        my $readonly = $mmObj->getVariableValue($newMasterHost,$dbPort,
                                    "show variables like 'read_only'",10);   
                                                                             
        # 若当前节点为主库，则不提升。节点为主库有两种情况：
        # a. show slave status为空，并且read_only=off
        if ( !$slaveStatusData && defined($readonly) && lc($readonly) eq "off"){
            # 不提升
            $exitCode = 2;
            $log->info("[instance:$dbPort] step1 $newMasterHost:$dbPort show slave status is empty and read_only=off");
            $log->info("[instance:$dbPort] step1 $newMasterHost:$dbPort is current master");
            $log->info("[instance:$dbPort] step1 exit promote");

            # 打印提升结果
            $op = MASTER_SKIP_PROMOTE;
            $opStatus .= " master ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append"); 
              
            return $exitCode;
        }
    
        # b. show slave status的Slave_IO_Running和Slave_SQL_Running都为NO，并且read_only=off
        if ( $slaveStatusData && %{$slaveStatusData} 
          && exists $slaveStatusData->{slave_io_running} 
          && exists $slaveStatusData->{slave_sql_running}
          && lc($slaveStatusData->{slave_io_running}) eq 'no'
          && lc($slaveStatusData->{slave_sql_running}) eq 'no' 
          && defined($readonly) && lc($readonly) eq "off" ) {
            # 不提升
            $exitCode = 2;
            $log->info("[instance:$dbPort] step1 $newMasterHost:$dbPort slave_io_running=no and slave_sql_running=no and read_only=off");
            $log->info("[instance:$dbPort] step1 $newMasterHost:$dbPort is current master");
            $log->info("[instance:$dbPort] step1 exit promote");

            # 打印提升结果
            $op = MASTER_SKIP_PROMOTE;
            $opStatus .= " master ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append"); 
                
            return $exitCode;          
        }
        
        # 当前节点是备主
        $log->info("[instance:$dbPort] step1 $newMasterHost:$dbPort is backup master, will promote");
          
        # 2. 检查备主复制配置
        $log->info();
        $log->info("[instance:$dbPort] step2: $newMasterHost:$dbPort start checking replication config");
        my $checkReplConf = $mmObj->checkBackupNodeReplConfig($newMasterHost,
                                            $oldMasterHost, $dbPort, 10 );
        if ( !$checkReplConf ){
            $log->error("[instance:$dbPort] step2 $newMasterHost:$dbPort is backup master");
            $log->error("[instance:$dbPort] step2 but replication config is error");
            $log->error("[instance:$dbPort] step2 failed");

            # 打印提升结果
            $op = REPL_CONFIG_ERROR_SKIP_PROMOTE;
            $opStatus .= " backup ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append"); 

            # 复制配置异常，不提升
            return $exitCode;
        }else{
            $log->info("[instance:$dbPort] step2 success");
        } 
    
        # 3. 检查备主复制线程
        $log->info();
        $log->info("[instance:$dbPort] step3: $newMasterHost:$dbPort start checking replication thread");       
        my $checkReplThread = $mmObj->checkReplThread( $newMasterHost,
                                                        $dbPort, 10 );
        if ( !$checkReplThread ) {
            $log->error("[instance:$dbPort] step3 $newMasterHost:$dbPort is backup master");
            $log->error("[instance:$dbPort] step3 but replication thread is error");
            $log->error("[instance:$dbPort] step3 failed");

            # 打印提升结果
            $op = REPL_THREAD_ERROR_SKIP_PROMOTE;
            $opStatus .= " backup ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append"); 
            
            # 复制线程异常，不提升
            return $exitCode;
        }else{
            $log->info("[instance:$dbPort] step3 success");
        } 
    
        # 4. 阻塞前端写入旧主
        $log->info();
        $log->info("[instance:$dbPort] step4: $oldMasterHost:$dbPort start set read_only=on"); 
        my $setOldMasterReadonly = $mmObj->executeSql($oldMasterHost, $dbPort,
                                            "SET GLOBAL READ_ONLY=ON", 10);
        if ( !$setOldMasterReadonly ) {
            $log->error("[instance:$dbPort] step4 failed");
        }else{
            $log->info("[instance:$dbPort] step4 success");
        }  
    
        # 5. 杀掉旧主mysql连接会话
        $log->info();
        $log->info("[instance:$dbPort] step5: $oldMasterHost:$dbPort is oldmaster");
        $log->info("[instance:$dbPort] step5: $oldMasterHost:$dbPort start killing connection");
        my $killOldMasterSessionTimeout = $myconfObj->get('killOldMasterSessionTimeout');
        if ( !$killOldMasterSessionTimeout ) {
            $log->warn("[instance:$dbPort] step5: get killOldMasterSessionTimeout failed, set it 30");
            $killOldMasterSessionTimeout = 30;
        } 
        $mmObj->killMysqlSession($oldMasterHost, $dbPort, $killOldMasterSessionTimeout); 
        $log->info("[instance:$dbPort] step5 success"); 
             
        
        # 6. 检查备主复制数据同步
        $log->info();
        $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort start checking replication data sync"); 
        my $newMasterCheckReplSyncRetryNum = $myconfObj->get('newMasterCheckReplSyncRetryNum');
        if ( !$newMasterCheckReplSyncRetryNum ) {
            $log->warn("get newMasterCheckReplSyncRetryNum failed, set it 3 times");
            $newMasterCheckReplSyncRetryNum = 3;
        }  
        my $newMasterWaitRetryTime = $myconfObj->get('newMasterCheckReplSyncWaitRetryTime'); 
        if ( !$newMasterWaitRetryTime ) { 
            $log->warn("get newMasterCheckReplSyncWaitRetryTime failed, set it 3s");
            $newMasterWaitRetryTime = 3;
        }
        my $dataConsistencyPriority = $myconfObj->get('dataConsistencyPriority');
        if ( !defined($dataConsistencyPriority) ) {
            $log->warn("get dataConsistencyPriority failed, set it 1");
            $dataConsistencyPriority = 1;
        }
        my $newMasterCheckReplSyncTimeout = $myconfObj->get('newMasterCheckReplSyncTimeout');
        if ( !defined($newMasterCheckReplSyncTimeout) ) {
            $log->warn("get newMasterCheckReplSyncTimeout failed, set it 60");
            $newMasterCheckReplSyncTimeout = 60;
        }
        
        # 判断是否数据一致性优先
        if ( $dataConsistencyPriority == 0 ){
            # 高可用优先 
            $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort dataConsistencyPriority is 0");
            $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort is high availability priority");  
                  
            for ( my $i=0; $i<$newMasterCheckReplSyncRetryNum; $i++ ) {
                $log->info("[instance:$dbPort] step6:  $i times doing checkReplSync");
                my $checkReplSync = $mmObj->checkReplSync($newMasterHost,$dbPort,
                                                      $newMasterCheckReplSyncTimeout);

                if ( !$checkReplSync ) {
                    # 备主数据未追上主库, 重试检查
                    if ($i == ($newMasterCheckReplSyncRetryNum-1)) {
                       # 最后一次重试, 备主数据未追上主库，强制提升
                       $log->warn("[instance:$dbPort] step6: $newMasterHost:$dbPort repl data doesn't catchup to master");
                       $log->warn("[instance:$dbPort] step6: $newMasterHost:$dbPort force itself promote to be master");
                       last;
                    }
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort repl data doesn't catchup to master");
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort sleep $newMasterWaitRetryTime s, and retry");
                    sleep $newMasterWaitRetryTime;
                    next;
                }else{
                    # 备主数据追上主库, 继续提升
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort repl data has catchup to master");
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort replication data sync is OK");
                    last;
                }
            }    
        } else {
            # mysql为数据一致性优先  
            $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort dataConsistencyPriority is 1");
            $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort is data consistency priority");
              
            my $stopFlagFile = $myconfObj->get('dataConsistencyStopFlagFile');
            my $maxWaitTime = $myconfObj->get('dataConsistencyMaxWaitTime');
            if ( !defined($maxWaitTime) ) {
                $log->warn("get dataConsistencyMaxWaitTime failed, set it 3600");
                $maxWaitTime = 3600;
            }        
            $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort maxWaitTime for dataConsistency: $maxWaitTime");      
            
            for ( my $i=0; $i<$maxWaitTime; $i++ ){             
                my $checkReplSync = $mmObj->checkReplSync($newMasterHost,$dbPort,10);
                if ( !$checkReplSync ) {
                    if ( -e $stopFlagFile ){
                      $log->warn("[instance:$dbPort] step6: $stopFlagFile is created, it will ignore dataConsistency");
                      $log->warn("[instance:$dbPort] step6: $newMasterHost:$dbPort force itself promote to be master now");
                      last;
                    }
                    if ( $i == ($maxWaitTime-1) ) {
                      $log->error("[instance:$dbPort] step6: $newMasterHost:$dbPort repl data doesn't catchup to master");
                      $log->error("[instance:$dbPort] step6: $newMasterHost:$dbPort execeed maxWaitTime: $maxWaitTime");
                      $log->error("[instance:$dbPort] step6: $newMasterHost:$dbPort stop promote");
                      $log->error("[instance:$dbPort] step6: failed");

                      # 打印提升结果
                      $op = REPL_DATA_LAG_SKIP_PROMOTE;
                      $opStatus .= " backup ". $op . " \n";
                      opStatus($myconfObj, $opStatus, "append"); 
    
                      return $exitCode;
                    }
                    sleep 1;
                    $log->info("[instance:$dbPort] step6: you can manual touch $stopFlagFile,it will ignore dataConsistency and force promote to master");  
                    next;
                }else{
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort repl data has catchup to master");
                    $log->info("[instance:$dbPort] step6: $newMasterHost:$dbPort replication data sync is OK");
                    last;
                }
            }
        }
        
        # 7.1 备主stop slave
        $log->info();
        $log->info("[instance:$dbPort] step7.1: $newMasterHost:$dbPort stop slave"); 
        my $stopSlave = $mmObj->executeSql($newMasterHost, $dbPort, "stop slave", 10);
        if ( !$stopSlave ) {
            $log->error("[instance:$dbPort] step7.1 failed");
        }else{
            $log->info("[instance:$dbPort] step7.1 success");
        } 
    
        # 7.2 备主执行flush logs
        $log->info();
        $log->info("[instance:$dbPort] step7.2: $newMasterHost:$dbPort flush logs"); 
        my $flushLog = $mmObj->executeSql($newMasterHost, $dbPort, "flush logs", 10);
        if ( !$flushLog ) {
            $log->error("[instance:$dbPort] step7.2 failed");
        }else{
            $log->info("[instance:$dbPort] step7.2 success");
        }
            
        # get masterStatus data for initSlave changeMasterTo 
        my $masterStatus = $mmObj->getMasterStatus($newMasterHost, $dbPort, 5); 
        
        # 8. 备主记录自己binlog日志同步点
        $log->info();
        $log->info("[instance:$dbPort] step8: $newMasterHost:$dbPort start log self binlog pos");
        $log->info("[instance:$dbPort] step8.1: $newMasterHost:$dbPort start log self binlog pos to DB");
    
        my $logBinlogPosToDb = $mmObj->logBinlogPosToDb($newMasterHost, $dbPort,"MASTER", 10);
        if ( !$logBinlogPosToDb ) {
            $log->error("[instance:$dbPort] step8.1 failed");
        }else{
            $log->info("[instance:$dbPort] step8.1 success");
        }
    
        $log->info("[instance:$dbPort] step8.2: $newMasterHost:$dbPort start log self binlog pos to file");
        my $logBinlogPosToFile = $mmObj->logBinlogPosToFile($newMasterHost, $dbPort, 10);
        if ( !$logBinlogPosToFile ) {
            $log->error("[instance:$dbPort] step8.2 failed");
        }else{
            $log->info("[instance:$dbPort] step8.2 success");
        }    
                    
        # 9. 备主记录自己slave info
        $log->info();
        $log->info("[instance:$dbPort] step9: $newMasterHost:$dbPort log self slave info");
        $log->info("[instance:$dbPort] step9.1: $newMasterHost:$dbPort log slave info to DB");
        my $logSlaveInfoToDb = $mmObj->logSlaveInfoToDb($newMasterHost,$dbPort,"MASTER", 10);
        if ( !$logSlaveInfoToDb ) {
            $log->error("[instance:$dbPort] step9.1 failed");
        }else{
            $log->info("[instance:$dbPort] step9.1 success");
        }  
    
        $log->info("[instance:$dbPort] step9.2: $newMasterHost:$dbPort log slave info to file");
        my $logSlaveInfoToFile = $mmObj->logSlaveInfoToFile($newMasterHost, 
                                                                   $dbPort, 10);
        if ( !$logSlaveInfoToFile ) {
            $log->error("[instance:$dbPort] step9.2 failed");
        }else{
            $log->info("[instance:$dbPort] step9.2 success");
        }

        # 10. 设置备主为可写, 正式成为新主库
        $log->info();
        $log->info("[instance:$dbPort] step10: $newMasterHost:$dbPort start set read_only=off"); 
        my $setMysqlWrite = $mmObj->executeSql($newMasterHost, $dbPort,
                                        "SET GLOBAL READ_ONLY=off", 10);

        if ( !$setMysqlWrite ) {
            $log->error("[instance:$dbPort] step10 failed");
            
            # 打印提升结果
            $op = PROMOTE_ERROR;
            $opStatus .= " backup ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append");                
        }else{
            $log->info("[instance:$dbPort]s step10 success");
            $exitCode = 1;
            
            # 打印提升结果
            $op = PROMOTE_SUCCESS;
            $opStatus .= " backup ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append");                
        }
        
        # 11. 新主库杀掉自己mysql连接会话 
        $log->info();
        $log->info("[instance:$dbPort] step11: $newMasterHost:$dbPort start kill mysql connections");   
        my $killMysqlSession = $mmObj->killMysqlSession($newMasterHost,$dbPort,10);
        $log->info("[instance:$dbPort] step11: success");
                                               
        # 12. 新主库开启event scheduler
        $log->info();
        $log->info("[instance:$dbPort] step12: $newMasterHost:$dbPort start turn on event scheduler"); 
        my $setEvent = $mmObj->turnOnEventScheduler($newMasterHost, $dbPort, 5);
        if ( !$setEvent ) {
            $log->error("[instance:$dbPort] step12 failed");
        }else{
            $log->info("[instance:$dbPort] step12 success");         
        }

        # 13. 新主库断开与旧主复制关系 
        $log->info();
        $log->info("[instance:$dbPort] step13: $newMasterHost:$dbPort start reset slave");
        my $newMasterResetSlave = $mmObj->executeSql($newMasterHost, $dbPort, 
                                                     "reset slave all", 10);                                          
        if ( !$newMasterResetSlave ) {
            $log->error("[instance:$dbPort] step13 failed");
        }else{
            $log->info("[instance:$dbPort] step13 success");
        }
        
        # 14. 打印新主库binlog同步点,方便手工change master to
        $log->info();
        $log->info("[instance:$dbPort] step14: print changeMasterTo SQL");
        
        my ( $masterLogFile,$masterLogPos );
        if ( $masterStatus && %{$masterStatus} 
            && exists $masterStatus->{file} 
            && exists $masterStatus->{position} ) 
        {
            # 从哈希变量$masterStatus获取新主库binlog同步点
            $masterLogFile = $masterStatus->{file};
            $masterLogPos = $masterStatus->{position};
        }
        
        # 获取复制账号和密码
        my $replUser = $myconfObj->get('dbUser');
        if ( !$replUser ) { 
            $log->error("[instance:$dbPort] step14: get repl user failed");
        }
        my $replPassword = $myconfObj->get('dbPassword');
        if ( !$replPassword ) { 
            $log->error("[instance:$dbPort] step14: get repl password failed");
        }
        $log->debug("[instance:$dbPort] step14: get repluser=$replUser replPassword=$replPassword"); 

        # 打印changeMasterTo SQL     
        my $sql = "CHANGE MASTER TO MASTER_HOST='" . $newMasterHost 
            . "',MASTER_PORT=" . $dbPort
            . ",MASTER_USER='" . $replUser
            . "',MASTER_PASSWORD='" . $replPassword
            . "',MASTER_LOG_FILE='" . $masterLogFile 
            . "',MASTER_LOG_POS=" . $masterLogPos;
            
        $log->info("[instance:$dbPort] step14: =======     change master to SQL:    ========   ");
        $log->info("$sql;");
    
        # 15. 调整旧主和其他从库指向新主库
        $log->info();
        $log->info("[instance:$dbPort] step15: $newMasterHost:$dbPort start initSlave");
        if ( $slaves && @{$slaves} ) {
            my $initSlaveObj = new InitSlave();                 
            $initSlaveObj->initSlave( $vrrpInstName,$masterStatus,$slaves,
                              $oldMasterHost,$newMasterHost,$dbPort );                              
        $log->info();
        $log->info("[instance:$dbPort] step15: finish");
        $log->info();
        } else {
            $log->info();
            $log->error("[instance:$dbPort] step15 no slave to init"); 
        }
    }else{   
        # 写vip不可用, 给定时间内ping不通网关，则不做提升
        $log->error("[instance:$dbPort] $newMasterHost:$dbPort found writeVip is unavailable, stop promote");
               
        # 打印提升结果
        $op = WRITEVIP_ERROR_SKIP_PROMOTE;
        $opStatus .= " backup ". $op . " \n";
        opStatus($myconfObj, $opStatus, "append");         
    }
    
    return $exitCode;
}

1;

