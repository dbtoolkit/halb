# Description: check write vip available
# Authors:  
#   zhaoyunbo

package CheckWvip;

use strict;
use warnings;

use POSIX qw(strftime);
use POSIX qw(:signal_h);
use Log::Log4perl;
use Exporter ();

our @ISA = qw(Exporter);

# 这里export函数，其他地方可以直接使用
our @EXPORT = qw(getWvip checkWvip getGateway pingWvip runCommand); 
our @VERSION = 1.0;


# @Description: 检查写vip是否可用
# @Param:  vrrp实例名
# @Return:  成功返回1，失败返回undef
sub checkWvip{
    my ($InstanceName) = @_;
    
    my $log = Log::Log4perl->get_logger("");
    
    # 获取网关ip地址
    my $gateway=getGateway();
    chomp($gateway);
    if ( !$gateway ){
        $log->error("get gateway failed");
        
        # 获取网关ip失败，返回undef
        return;
    }
    $log->info("get gateway success, gateway: $gateway");
    
    # 获取写vip地址
    my $wvip='';
    # 失败重试10次，每次sleep 1s
    my $i = 1;
    while( $i<11 ){
        $log->info("the ($i) times check");
        $wvip = getWvip($InstanceName);
        if ($wvip){
            # 获取写vip地址成功
            $log->info("get wvip success, wvip is $wvip");
            last;
        }
        $i++;
        sleep(1);
    }
    
    # 判断是否已成功获取写vip    
    if ( $wvip ){        
        # 获取写vip成功，开始ping网关
        my $ping_res = pingWvip($wvip, $gateway);
        if ( $ping_res==1 ){
            $log->info("ping wvip success");
            
            # ping网关成功，返回1
            return 1;     
        } elsif( $ping_res==0 ){
            $log->error("ping wvip failed");
            
            # ping网关失败，返回undef
            return; 
        }
    }else{
        $log->error("get wvip failed, wvip not ok on this machine");
        
        # 获取写vip地址失败，返回undef
        return;
    }
}

# @Description: 获取写vip
# @Param: vrrp实例名
# @Return:  成功返回写vip  失败返回undef
sub getWvip{
    my ($InstanceName) = @_;

    my $log = Log::Log4perl->get_logger(""); 
    $log->info("current instanceName is $InstanceName");
    
    # 获取本机所有ip地址列表
    my @ipList='';
    my $cmd=qq(ip -4 -o a s);
    my @ipData = split("\n", runCommand($cmd, 10));
    if (@ipData) {
        foreach my $line(@ipData){
            if($line =~ /\s+inet\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\/\d+/){
                push @ipList, $1;
            }
        }
        chomp(@ipList);         
    }else{
        $log->error("excute command: $cmd failed"); 
        
        # 获取本机ip地址列表失败，返回undef  
        return;
    }

    # 获取haconf配置文件中的写vip
    my $haconf='/etc/keepalived/haconf';
    my $command=qq( awk 'tolower(\$8)==tolower("$InstanceName"){print \$9}' $haconf | uniq );
    my $haconf_wvip=`$command`;
    chomp($haconf_wvip);
    
    # 机器上实际启动的写vip
    my $real_wvip;
    if($haconf_wvip){
        $log->info("haconf wvip is $haconf_wvip");
        foreach $real_wvip(@ipList){
            if($real_wvip eq $haconf_wvip){
                
                # 若实际启动的写vip为配置文件中对应的写vip, 则成功返回写vip 
                $log->info("get wvip success, wvip: $real_wvip");   
                return $real_wvip;            
            }
        }
        # 否则失败返回undef
        $log->info("wvip not exist on current node");
        return;
    }else{
        $log->error("wvip not in haconf");
        
        # 失败返回undef
        return;    
    }        
}

# @Description: 获取网关ip
# @Param: 
# @Return:  成功返回网关ip  失败返回undef
sub getGateway{
    my $log = Log::Log4perl->get_logger("");
    
    my $command=qq(netstat -rn | grep "UG" | awk '{print \$2}' | uniq | head -n 1); 
    my $gateway=runCommand($command, 5);
    
    return $gateway;
}

# @Description:  写vip ping网关
# @Param: 写vip  网关ip
# @Return:  成功返回1，失败返回0
sub pingWvip{
    my($wvip, $gateway) = @_;    
    
    my $log = Log::Log4perl->get_logger("");

    my $command = qq(ping -I $wvip $gateway -c 1 -W 1);
    $log->info("$command");
    
    $log->info("wvip start pinging gateway");
    
    # ping网关失败重试次数
    my $retry_times = 16;
    for ( my $i=1; $i<$retry_times; $i++ ){
        # 执行命令   
        my $ping_res = runCommand($command, 2);
        my @ping_res = split("\n", $ping_res);
        if(!@ping_res){
            $log->error("ping wvip $i time failed");
            
            if ( $i == ($retry_times-1) ){           
                
                # 最后一次ping网关失败，返回0
                return 0;
            }
        }else{
            foreach(@ping_res){
                if($_ =~ /100% packet loss/){
                    # ping wvip failed
                    $log->error("ping wvip $i time failed, ping packet 100% lost");
                    
                    if ( $i == ($retry_times-1) ){
                        
                        # 最后一次ping网关失败，返回0
                        return 0;
                    }
                }elsif($_ =~ /0% packet loss/ || $_ =~ /ttl=/){
                    $log->info("ping wvip $i time success");
                    
                    # ping网关成功，返回1
                    return 1;
                }
            }        
        }
    }
}

# @Description: 执行系统命令
# @Param: 命令
# @Return:  成功返回命令执行结果 失败返回undef
sub runCommand{
    my ($command, $timeout) = @_;
    
    my $log = Log::Log4perl->get_logger("");
     
    # 超时处理
    my $timeoutMsg = "CMD_TIMEOUT";     
    my $sigset = POSIX::SigSet->new( SIGALRM );  # signals to mask in the handler
    my $action = POSIX::SigAction->new( sub { die $timeoutMsg; },$sigset,);
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction ); 

    # 执行命令
    my $res;    
    eval {
        alarm $timeout;
        $res=`$command`;
        alarm 0;    
    };
    alarm 0; 
    sigaction( SIGALRM, $oldaction );  # restore original signal handler
    
    # 异常处理
    if ( $@ ) {
        if ( $@ =~ /$timeoutMsg/ ) {
            $log->error("run command: $command timeout");
        }else{
            $log->error("run command: $command failed");
        }
        undef $@;
        
        # 返回undef
        return;
    }
    
    # 返回命令执行结果
    $log->info("run command: $command success");
    return $res;
}

1;

