# Description:  demote mysql to be backup
# Authors:  
#   zhaoyunbo

package Demote;

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
use OpStatus;
use ManagerUtil;

# 字符串常量含义
use constant DEMOTE_ERROR => "demote error null";  # 降级失败
use constant DEMOTE_SUCCESS => "demote success null";   # 降级成功
use constant MASTER_SKIP_DEMOTE => "demote skip it_is_current_master"; # master不降级

# @Description: 构造函数
# @Param: 
# @Return: $self 
sub new {
    my ( $class, %args ) = @_;
    my $self = {};  # create a hash ref   

    bless ( $self, $class );  # statement object type
    return $self;  # return an object 
}


# @Description: 降级mysql为备库 
# @Param: ($vrrpInstName,$slaves,$oldMasterHost,$newMasterHost,$dbPort)
# @Return: 1:成功  2:忽略  0:失败
sub main {
    my ($self,$vrrpInstName,$dbHost,$dbPort) = @_;
    
    # 降级结果标识
    my $exitCode = 0;
    
    my $log = Log::Log4perl->get_logger("");
    
    my $myconfObj = new Myconfig();
    my $mysqlConnObj = new MysqlConn( myconfObj => $myconfObj );
    my $mmObj = new MysqlManager( 
                    myconfObj => $myconfObj,
                    mysqlConnObj => $mysqlConnObj
                );

    # 用于打印降级结果
    my $opStatus = $vrrpInstName." ".$dbHost." ".$dbPort; 
    my $op = "";

    # 检查角色, 若为正常主库，则不降级
    $log->info("[instance:$dbPort] $dbHost:$dbPort checking writeVip available"); 
    my $checkWriteVip = checkWvip($vrrpInstName);
    if (defined($checkWriteVip) && $checkWriteVip == 1){     
        # (1) 写vip可用，且ping通网关
        $log->info("[instance:$dbPort] $dbHost:$dbPort writeVip is ok"); 
                      
        # (2) 主库节点有两种情况:
        #  a. show slave status为空，并且read_only=off
        #   或者
        #  b. show slave status的Slave_IO_Running和Slave_SQL_Running都为NO，并且read_only=off 
        $log->info("[instance:$dbPort] step1: $dbHost:$dbPort start checking"); 
        $log->info();
        my $slaveStatusData = $mmObj->getSlaveStatus($dbHost,$dbPort,10);
        my $readonly = $mmObj->getVariableValue($dbHost,$dbPort,
                                    "show variables like 'read_only'",10);   
    
        # a. show slave status为空，并且read_only=off
        if ( !$slaveStatusData && defined($readonly) && lc($readonly) eq "off"){
            # 主库不降级
            $exitCode = 2;
            $log->info("[instance:$dbPort] step1 $dbHost:$dbPort show slave status is empty and read_only is off");
            $log->info("[instance:$dbPort] step1 $dbHost:$dbPort is current master");
            $log->info("[instance:$dbPort] step1 exit demote");

            # 打印降级结果
            $op = MASTER_SKIP_DEMOTE;
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
            # 主库不降级
            $exitCode = 2;
            $log->info("[instance:$dbPort] step1 $dbHost:$dbPort slave_io_running=no and slave_sql_running=no and read_only is off");
            $log->info("[instance:$dbPort] step1 $dbHost:$dbPort is current master");
            $log->info("[instance:$dbPort] step1 exit demote");

            # 打印降级结果
            $op = MASTER_SKIP_DEMOTE;
            $opStatus .= " master ". $op . " \n";
            opStatus($myconfObj, $opStatus, "append"); 
    
            return $exitCode;          
        }
        
        # 打印read_only值
        if ($readonly){
            $log->info("[instance:$dbPort] $dbHost:$dbPort read_only is $readonly");
        }
    }
    # 其他所有情况做降级处理，打开只读，前端无法写入
    $log->info("[instance:$dbPort] $dbHost:$dbPort writeVip is not ok");
    
    # 2. 设置当前节点为只读
    $log->info("[instance:$dbPort] step2: $dbHost:$dbPort start set read_only=on"); 
        
    my $setReadonlyOnTimeout = $myconfObj->get('setReadonlyOnTimeout'); 
    if ( !$setReadonlyOnTimeout ) { 
        $log->warn("[instance:$dbPort] get setReadonlyOnTimeout failed, set setReadonlyOnTimeout=30");
        $setReadonlyOnTimeout = 30;
    }
    
    my $setReadonlyOn = $mmObj->executeSql($dbHost, $dbPort, 
                      "set global read_only=on", $setReadonlyOnTimeout);
    if ( !$setReadonlyOn ){
        $log->error("[instance:$dbPort] step2 failed");
        
        # 打印降级结果
        $op = DEMOTE_ERROR;
        $opStatus .= " node ". $op . " \n";
        opStatus($myconfObj, $opStatus, "append"); 
    }else{
        # 设置read_only=on成功, 降级成功
        $exitCode = 1;
        $log->info("[instance:$dbPort] step2 success");

        # 打印降级结果
        $op = DEMOTE_SUCCESS;
        $opStatus .= " node ". $op . " \n";
        opStatus($myconfObj, $opStatus, "append"); 
    }
        
    # 3. 关闭event scheduler
    $log->info("[instance:$dbPort] step3: $dbHost:$dbPort start turning off event scheduler");
    my $turnOffEventSche = $mmObj->executeSql($dbHost, $dbPort, 
                      "set global event_scheduler=off", 5);
    if ( !$turnOffEventSche ) {
        $log->error("[instance:$dbPort] step3 failed");
    }else{
        $log->info("[instance:$dbPort] step3 success");
    }
    
    # 4. 检查mysql_identity数据库和切换日志信息表rep_log_info是否存在
    $log->info("[instance:$dbPort] step4 start checking mysql_identity and rep_log_info exist");
    eval{
        my $dbName = $myconfObj->get('dbName'); # get dbName
        if ( !$dbName ) { 
            $log->warn("[instance:$dbPort] $dbHost:$dbPort get dbName failed, set dbName=mysql_identity"); 
            $dbName = "mysql_identity";
        }

        my $tableName = $myconfObj->get('tableName'); # get tableName
        if ( !$tableName ) { 
            $log->warn("[instance:$dbPort] $dbHost:$dbPort get tableName failed, set tableName=rep_log_info");
            $tableName = "rep_log_info";
        }
             
        my $checkDbExist = $mmObj->checkDbExist($dbName,$dbHost,$dbPort,5);
        if ( !$checkDbExist ) {
            $log->error("[instance:$dbPort] $dbHost:$dbPort checkDbExist failed");
            $mmObj->createDb($dbName,$dbHost,$dbPort,5);
        }else{
            $log->info("[instance:$dbPort] $dbHost:$dbPort checkDbExist success");
        }
    
        my $checkTableExist = $mmObj->checkTableExist($dbName,$tableName,
                                                       $dbHost, $dbPort,5);
        if ( !$checkTableExist ) {
            $log->error("[instance:$dbPort] $dbHost:$dbPort checkTableExist failed");
            $mmObj->createTable($dbName,$tableName,$dbHost,$dbPort,5);
        }else{
            $log->info("[instance:$dbPort] $dbHost:$dbPort checkTableExist success");
        }    
    };
    if($@){
        $log->error("[instance:$dbPort] step4 something error");
        undef $@; 
    }
    $log->info("[instance:$dbPort] step4 success");

    # 5. 记录自己binlog同步点 
    $log->info("[instance:$dbPort] step5: $dbHost:$dbPort start logging self binlog pos");  
    $log->info("[instance:$dbPort] step5.1: $dbHost:$dbPort start logging self binlog pos to DB");
    my $logBinlogPosToDb = $mmObj->logBinlogPosToDb($dbHost, $dbPort,
                                                  "BACKUP", 10);
    if ( !$logBinlogPosToDb ) {
        $log->error("[instance:$dbPort] step5.1 failed");
    }else{
        $log->info("[instance:$dbPort] step5.1 success");
    }
         
    $log->info("[instance:$dbPort] step5.2: $dbHost:$dbPort start logging self binlog pos to file");
    my $logBinlogPosToFile = $mmObj->logBinlogPosToFile($dbHost, $dbPort, 10);
    if ( !$logBinlogPosToFile ) {
        $log->error("[instance:$dbPort] step5.2 failed");
    }else{
        $log->info("[instance:$dbPort] step5.2 success");
    }    
    
    # 6. 记录自己slave info 
    $log->info("[instance:$dbPort] step6: $dbHost:$dbPort start logging self slave info to file");
    my $logSlaveInfoToFile = $mmObj->logSlaveInfoToFile($dbHost, $dbPort, 10);
    if ( !$logSlaveInfoToFile ) {
        $log->error("[instance:$dbPort] step6 failed");
    }else{
        $log->info("[instance:$dbPort] step6 success");
    }
    
    return $exitCode;
}

1;

