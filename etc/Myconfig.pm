# Description: halb config file
# Authors:
#   zhaoyunbo

package Myconfig;

use strict;
use warnings;

# @description: load config data

sub load {
    my $self = shift;
    
    # 切换主脚本
    ###########---------- switchover.pl -------------###########
    
    $self->{switchover} = "on";   # 取值: on|off, 只有值为"on"，才会进行主从切换
        
    ###########---------- 所有模块 -------------###########   
     
    $self->{dbHost} = '127.0.0.1';           # db host
    $self->{dbUser} = 'slave';               # 复制账号用户名
    $self->{dbPassword} = 'xxxxxxx';     # 复制账号密码
    $self->{dbName} = 'mysql_identity';      # 高可用用到的数据库
    $self->{tableName} = 'rep_log_info';     # 切换日志表  
    $self->{home} = '/etc/keepalived';       # 高可用目录           
    $self->{haconfFile} = '/etc/keepalived/haconf';    # 高可用全局配置文件
    $self->{dataDir} = '/etc/keepalived/status';       # 状态文件目录  
    $self->{opStatusFile} = '/etc/keepalived/status/opStatus';       # 状态文件

    # 连接mysql
    ###########---------- MysqlConn.pm -------------###########  
    
    $self->{dbConnTimeout} = 5;           # 连接mysql超时时间
    $self->{dbConnectRetryNum} = 3;        # 连接mysql失败重试次数
    $self->{dbReconnectWaitTime} = 1;     # 重连mysql等待时间间隔
    $self->{retryType} = "sleep";          # 该值不需要更改
    
    # 降级
    ###########---------- Demote.pm -------------###########  
    
    $self->{setReadonlyOnTimeout} = 60;    # 执行set read_only=on超时时间
    
    # 提升
    ###########---------- Promote.pm -------------###########

    $self->{killOldMasterSessionTimeout} = 30;         # 杀mysql旧主库应用账号连接会话超时时间      
    $self->{setReadonlyOffTimeout} = 60;               # 执行set read_only=off超时时间
        
    $self->{dataConsistencyPriority} = 0;  # "1"为数据一致性优先,备主复制数据没有应用完成,不提升为主库     
    
    # 一致性优先参数
    # 若备主复制数据没有应用完成, 则会循环检查数据应用情况, 直到最大等待时间, 数据应用未完成，则不做提升
    $self->{dataConsistencyMaxWaitTime} = 3600; 
    # 手工touch /tmp/force_promote.flag，让备主强制提升为主库     
    $self->{dataConsistencyStopFlagFile} = '/tmp/force_promote.flag'; 
    
    # 高可用优先参数
    $self->{newMasterCheckReplSyncRetryNum} = 120;       # 检查备主复制同步失败重试次数     
    $self->{newMasterCheckReplSyncWaitRetryTime} = 1;  # 检查备主复制同步失败重试时间间隔  
    $self->{newMasterCheckReplSyncTimeout} = 60;       # 检查备主复制同步超时时间       
    
    # 初始化从库
    ###########---------- InitSlave.pm -------------###########    

    $self->{initSlaveRetryNum} = 5;           # 初始化从库失败重试次数
    $self->{initSlaveWaitRetryTime} = 1;        # 初始化从库失败重试时间间隔 
    $self->{checkReplSyncRetryNum} = 5;       # 检查从库复制同步失败重试次数
    $self->{checkReplSyncWaitRetryTime} = 1;    # 检查从库复制同步失败重试时间间隔   
    $self->{logIntervalTime} = 600;             # 该值不需要更改

    # 检查mysql健康
    ###########---------- check_mysql.pl  -------------###########
        
    $self->{checkHost} = '127.0.0.1';     # 高可用账号连接数据库主机
    $self->{checkUser} = 'mysqlha';          # 高可用账号用户名
    $self->{checkPassword} = 'xxxxxx';      # 高可用账号密码
    $self->{checkMysqlWrite} = 0;         # 检查mysql可写探测 
    $self->{checkMysqlRetryTimes} = 10;    # 检查mysql失败重试次数
    $self->{checkMysqlWaitTime} = 1;      # 检查mysql失败重试时间间隔

                      
    # 检查realserver健康
    ###########---------- ping_realserver.pl -------------###########
    
    $self->{checkRsTimeout} = 10;      # 检查超时时间
    $self->{checkRsRetryTimes} = 6;    # 检查失败重试次数
    $self->{checkRsIntervalTime} = 1;  # 检查失败重试时间间隔（秒）
    $self->{rsMaxSlaveLag} = 1800;      # 允许从库最大复制延迟时间，slave lag大于该值，会摘除 
    
    # 注意:  检查mysql和realserver健康时,
    # 检查总时间大约为 重试次数 x 重试时间间隔
    # 检查总时间 < interval值(keepalived.conf的vrrp_script interval值)
    # 若检查总时间超过interval值，messages日志会报如下错误：
    # Keepalived_healthcheckers[3514]: Process [46566] didn't respond to SIGTERM
	
    # 检查脑裂
    ###########---------- check_sb.pl -------------###########
    
    $self->{checkSbTimeout} = 2;      # 单次检查超时时间
    $self->{checkSbRetryTimes} = 5;    # 检查失败重试次数
    
}

sub new {
    my ( $class, %args ) = @_;

    my $self = {};  # allocate new hash for object
    bless( $self, $class );  # statement object type

    # load config
    $self->load();

    return $self;
}

sub get {
    my ( $self, $key ) = @_;
    return $self->{$key};
}

sub set {
    my ( $self, $key, $value ) = @_;
    $self->{$key} = $value;
}

1;

