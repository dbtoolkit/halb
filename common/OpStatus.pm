# Description:  log operate status
# Authors:  
#   zhaoyunbo

package OpStatus;

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl;
use Exporter ();

our @ISA = qw(Exporter);
our @EXPORT = qw( opStatus ); 
our @VERSION = 1.0;

# @Description: 记录操作结果
# @Param: ($myconfObj $message $mode)
# @Return: 成功返回1 失败返回undef
sub opStatus {
	my ( $myconfObj, $message, $mode ) = @_;
	
	my $log = Log::Log4perl->get_logger("");
	
	my $file;
	eval {
		$file = $myconfObj->get('opStatusFile');
		if ( !defined($file) ) { 
			$log->error("Get $file failed."); 
		}
		my $dataDir = $myconfObj->get('dataDir');
		# 状态目录不存在，自动创建目录
		if ( ! -d $dataDir ) {
			eval {
				$log->info("$dataDir not exists, create it now.");
				mkdir($dataDir,0755);
			}; 
			if ($@) {
				$log->error("Create $dataDir failed.");
				undef $@;
			}
			$log->info("Create $dataDir success.");	
		}
		my $FH;
		if ( defined($mode) ) {
			my $modeStr;
			if ( lc $mode eq "trunc" ) {
				$modeStr = '>';
			} elsif ( lc $mode eq "append" ) {
				$modeStr = '>>';
			} elsif ( lc $mode eq "read" ) {
				$modeStr = '<';
			} else {
				$modeStr = '<';
				$log->error("Sub function logInitStatus get unknown \$mode.");
			}
			
			if ( !open $FH, $modeStr, $file ) { 
				$log->error("Open $file failed."); 
			}
			print $FH $message;
			if ($FH) { close $FH; }
		
		} else {
			$log->error("sub function logInitStatus get no \$mode.");
		}
	}; 
	if ($@) {
		$log->error("Write $message to $file failed.");
		undef $@;
		
		return; 
	} 
	
	if ( !$message ) {
		$log->info("clean $file success");
	}
	
	return 1;
}

1;

