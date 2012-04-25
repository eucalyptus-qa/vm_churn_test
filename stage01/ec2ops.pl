use List::Util qw[min max];

$prefix = "euca";
$runat = "runat 30";
%cleanup_artifacts, %static_artifacts, %current_artifacts;
$remote_pre = "";
$remote_post = "";
$trycount = 180;
$instance_run_timeout=600;
$pingtries = 30;
$ofile = "ubero";
%masters, %slaves, %roles, %cc_has_broker;
$use_virtio = 0;
$networkmode="";
$ismanaged = 1;
$isha = 0;
$cleanup = 1;
$library_sleep = 0;
$devname = "/dev/sdc";
$emitype = "instancestoreemi";
$keypath = ".";
$bfe_image = "http://192.168.7.65/bfebs-image/bfebs.img";
$piperetries = 1;
$imgfile="";

sub save_volid {
    my $vol = shift @_;
    if ($vol =~ /vol/ && -d "../etc/" ) {
	open(RFH, ">>../etc/vols.lst");
	print RFH "$vol\n";
	close(RFH);
    }
    return(0);
}

sub setinstanceruntimeout {
    my $timeout = shift @_ || 600;
    if ($timeout > 0 && $timeout < 86400) {
	$instance_run_timeout = $timeout;
    }
}   

sub exit_if_not_ha {
    my $component = shift @_ || "CLC";

    if ($masters{$component} && $slaves{$component}) {
	return(0);
    }
    doexit(1, "WARN: non-ha configuration detected for component '$component', exiting 1\n");
}

sub seteucaconf {
    my $key = shift @_;
    my $val = shift @_;

    if (!$key || !$val) {
	doexit(1, "ERROR: invalid key/val passed to seteucaconf ($key/$val)\n");
    }

    $cmd = "$runat $remote_pre 'sed -i \"s/DISABLE_DNS.*/$key=\\\"$val\\\"/\" /opt/eucalyptus/etc/eucalyptus/eucalyptus.conf' $remote_post";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	$cmd = "$runat $remote_pre 'sed -i \"s/DISABLE_DNS.*/$key=\\\"$val\\\"/\" /etc/eucalyptus/eucalyptus.conf' $remote_post";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {

	    doexit(1, "FAILED: could not set property $key to $val\n");
	}
    }
    return(0);
    
}

sub setproperties {
    my $key = shift @_;
    my $val = shift @_;

    if (! $key || ! $val || $key eq "" || $val eq "") {
	doexit(1, "ERROR: no key/val to set ($key/$val)\n");
    }
    
    $cmd = "$runat $remote_pre euca-modify-property -p $key=$val $remote_post";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: could not set property $key to $val\n");
    }
    return(0);
}

sub setzone {
    my $zone = shift @_ || "default";
    if ($zone ne "default" && $static_artifacts{"availabilityzones"} =~ /$zone/) {
	$current_artifacts{"availabilityzone"} = $zone;
    }
    return(0);
}

sub setpingtries {
    my $tries = shift @_ || 30;
    if ($tries > 0 && $tries < 9000) {
	$pingtries = $tries;
    }
    return(0);
}

sub setkeypath {
    $keypath = shift @_ || ".";
    if (! -d "$keypath") {
	$keypath = ".";
	return(1);
    }
    return(0);
}

sub setcleanup {
    $cleanup = shift @_ || "yes";
    if ($cleanup eq "no") {
	$cleanup = 0;
    } else {
	$cleanup = 1;
    }
    return(0);
}

sub settrycount {
    my $tries = shift @_ || 180;
    if ($tries > 0 && $tries < 9000) {
        $trycount = $tries;
    }
    return(0);
}

sub setfailuretype {
    #options are script, net, reboot
    my $type = shift @_ || "script";
    if ($type ne "script" && $type ne "net" && $type ne "reboot") {
	doexit(1, "FAILED: invalid failure type $type\n");
    }

    $current_artifacts{"failtype"} = $type;
    return(0);
}

sub install_ec2_api_tools {
    my $filever = shift @_ || "latest";
    my $file, $ver;

    if ($filever eq "latest") {
	$ver = "ec2-api-tools";
	$file = "$ver.zip";
    } else {
	$ver = $filever;
	$file = $filever . ".zip";
    }

    if ( ! -f "$file" ) {
	my $url = "http://s3.amazonaws.com/ec2-downloads/$file";
	my $cmd = "wget $url";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAILED: could not download ec2 api tools from '$url'\n");
	}
    }

    run_command("scp -o StrictHostKeyChecking=no $file root\@$current_artifacts{remoteip}:/tmp/");
    run_command("ssh -o StrictHostKeyChecking=no root\@$current_artifacts{remoteip} 'cd /tmp/; unzip -o $file'");
    
    if ($filever eq "latest") {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$current_artifacts{remoteip} 'ls -1ad /tmp/ec2-api-tools\*/'";
    } else {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$current_artifacts{remoteip} 'ls -1ad /tmp/$ver/'";
    }
    my ($rc, $crc, $buf) = piperun($cmd, "grep ec2-api-tools | tail -n 1", "ubero");
    if ($rc || $crc || !$buf) {
	doexit(1, "FAILED: could not find ec2 API tools location on remote machine\n");
    }
    
    $current_artifacts{"ec2apilocation"} = "$buf";
    setremote();
    return(0);
}

sub use_ec2_api_tools {
    setprefix("ec2");    
}

sub use_euca2ools {
    setprefix("euca");
}


sub control_component {
    my $ftype = $current_artifacts{"failtype"} || "script";
    if ($ftype eq "script") {
	return(control_component_script(@_));
    } elsif ($ftype eq "net") {
	return(control_component_net(@_));
    } elsif ($ftype eq "reboot") {
	return(control_component_reboot(@_));
    } else {
	doexit(1, "FAILED: could not find control driver for failtype $ftype\n");
    }
    return(0);
}

sub control_component_reboot {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC00";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	print "\tWARN: could not detemine component ip ($component, $rank, $op), skipping\n";
	return(0);
    }
    $current_artifacts{"controlip"} = $ip;

    if ($op ne "STOP") {
	my $done=0;
	my $i;
	for ($i=0; $i<300 && !$done; $i++) {
	    print "\ttesting network connectivity: $ip\n";
	    $cmd = "$runat ping -c 1 $ip";
	    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	    if (!$rc && !$crc) {
		$done++;
	    }
	    sleep(1);
	}
	if (!$done) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
	}
	sleep(30);
	
	$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip 'ntpdate pool.ntp.org'";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    print "WARN: could not run time sync ($cmd, $rc, $crc, $buf)\n";
	}

	return(control_component_script($op, $component, $rank));
    }
    
    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip 'reboot -f >/dev/null 2>&1 </dev/null &'";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    return(0);    
}

sub control_component_net {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	print "\tWARN: could not detemine component ip ($component, $rank, $op), skipping\n";
	return(0);
    }
    $current_artifacts{"controlip"} = $ip;
    
    if ($op ne "STOP") {
	my $done=0;
	my $i;
	for ($i=0; $i<120 && !$done; $i++) {
	    print "\ttesting network connectivity: $ip\n";
	    $cmd = "$runat ping -c 1 $ip";
	    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	    if (!$rc && !$crc) {
		$done++;
	    }
	    sleep(1);
	}
	if (!$done) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
	}

	return(0);
    }

    $cmd = "$runat scp -o StrictHostKeyChecking=no cyclenet.pl root\@$ip:/tmp/";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip '/tmp/cyclenet.pl 120 $ip >/dev/null 2>&1 </dev/null &'";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    return(0);
}

sub control_component_script {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	print "\tWARN: could not detemine component ip ($component, $rank, $op), skipping\n";
	return(0);
    }
    $current_artifacts{"controlip"} = $ip;

    $oldrunat = $runat;
    setrunat("runat 120");
    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip '";
    if ($component =~ /CC\d+/) {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-cc ";
	if ($op eq "STOP") {
	    $cmd .= "cleanstop";
	    $cleancmd = "$runat scp -o StrictHostKeyChecking=no cleannet.pl root\@$ip:/tmp/";
	    my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	    if ($rc || $crc) {
		doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	    }

	    $cleancmd = "$runat scp -o StrictHostKeyChecking=no ../input/2b_tested.lst root\@$ip:/tmp/";
	    my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	    if ($rc || $crc) {
		doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	    }
	    $cleancmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip /tmp/cleannet.pl /tmp/2b_tested.lst";
	} else {
	    $cmd .= "cleanstart";
	}
	if ($cc_has_broker{"$component"}) {
	    $cmd .= "; $current_artifacts{eucahome}/etc/init.d/eucalyptus-cloud ";
	    if ($op eq "STOP") {
		$cmd .= "stop";
	    } else {
		$cmd .= "start";
	    }
	}
    } elsif ($component =~ /NC\d+/) {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-nc ";
	if ($op eq "STOP") {
	    $cmd .= "stop";
	} else {
	    $cmd .= "start";
	}	
    } else {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-cloud ";
	if ($op eq "STOP") {
	    $cmd .= "stop";
	} else {
	    $cmd .= "start";
	}		
    }
    $cmd .= "'";

    my ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
    if (($crc || $rc)) {
	print "WARN: failed to $op component ($crc, $rc, $buf)\n";
    }
    if ($cleancmd) {
	my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	}
    }
    setrunat("$oldrunat");
    return(0);
}

sub setrunat {
    my $newrunat = shift @_ || "runat 30";

    $runat = $newrunat;

    return(0);
}

sub print_all_metadata {
    print "cleanup_artifacts: \n";
    foreach $key (keys(%cleanup_artifacts)) {
	$val = $cleanup_artifacts{$key};
	print "\t$key=$val\n";
    }
    print "static_artifacts: \n";
    foreach $key (keys(%static_artifacts)) {
	$val = $static_artifacts{$key};
	print "\t$key=$val\n";
    }
    print "current_artifacts: \n";
    foreach $key (keys(%current_artifacts)) {
	$val = $current_artifacts{$key};
	print "\t$key=$val\n";
    }
}

sub describe_services {
    
    $oldrunat = $runat;
    setrunat("runat 10");
    
    if ($masters{CLC}) {
	$cmd = "$runat ping -c 1 $masters{CLC}";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    print "WARN: cannot ping $masters{CLC} to determine service status\n";
	} else {
	    setremote($masters{CLC});
	    $cmd = "$runat $remote_pre if netstat -tan | grep 8443 >/dev/null ; then euca-describe-services --system-internal; else exit 1; fi $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	    if ($crc || $rc) {
		print "WARN: describe-services failed on $masters{CLC}\n";
	    } else {
		$current_artifacts{"master_ds_buf"} = $buf;
	    }
	}
    }
    
    if ($slaves{CLC}) {
	$cmd = "$runat ping -c 1 $slaves{CLC}";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    print "WARN: cannot ping $slaves{CLC} to determine service status\n";
	} else {
	    setremote($slaves{CLC});
	    $cmd = "$runat $remote_pre if netstat -tan | grep 8443 >/dev/null ; then euca-describe-services --system-internal; else exit 1; fi $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	    if ($crc || $rc) {
		print "WARN: describe-services failed on $slaves{CLC}\n";
	    } else {
		$current_artifacts{"slave_ds_buf"} = $buf;
	    }
	}
    }
    setrunat("$oldrunat");
    return(0);
}

sub find_real_master {
    my $component = shift @_ || "CLC";
    my $service = "";
    if ($component =~ "CLC") {
	$service = "eucalyptus";
    } elsif ($component =~ "SC") {
	$service = "storage";
    } elsif ($component =~ "CC") {
	$service = "cluster";
    } elsif ($component =~ "WS") {
	$service = "walrus";
    } elsif ($component =~ "ARB") {
        $service = "arbitrator";
    } else {
	return(0);
    }

    print "\nLooking for enabled $component\n";
    my $enabled_clc = "";
    if ($masters{$component}) {
	open(FH, ">/tmp/ec2ops.out.$$");
	print FH "$current_artifacts{master_ds_buf}\n";
	close(FH);
	$cmd = "cat /tmp/ec2ops.out.$$";
	if ($service ne "arbitrator") {
	    print "\tsearching for enabled service=$service\n";
	    ($crc, $rc, $buf) = piperun($cmd, "egrep -e 'SERVICE[[:space:]]+$service' | awk '{print \$5, \$7, \$8}' | grep ENABLED | grep $masters{$component} | head -n 1", "ubero");
	    print "\tfound rc=$rc, crc=$crc, buf=$buf\n";
	} else {
	    print "RUNNING: $cmd : $masters{$component} : $service\n";
	    system("$cmd");
	    ($crc, $rc, $buf) = piperun($cmd, "egrep -e 'SERVICE[[:space:]]+$service' | awk '{print \$5, \$7, \$8}' | grep $masters{$component} | head -n 1", "ubero");
	    print "DONE: $crc $rc |$buf|\n";
	}
	if ($crc) {
	    print "\t$component $masters{$component}: cannot determine status\n";
	} elsif ($rc || !$buf || $buf eq "") {
	    print "\t$component $masters{$component}: is not enabled\n";
	} else {
	    print "\t$component $masters{$component}: is enabled\n";
	    $enabled_clc = $masters{$component};
	}
    }
    if ($slaves{$component}) {
	if ($component eq "CLC") {
	    open(FH, ">/tmp/ec2ops.out.$$");
	    print FH "$current_artifacts{slave_ds_buf}\n";
	    close(FH);
	} else {
	    open(FH, ">/tmp/ec2ops.out.$$");
	    print FH "$current_artifacts{master_ds_buf}\n";
	    close(FH);
	}
	$cmd = "cat /tmp/ec2ops.out.$$";
	if ($service ne "arbitrator") {
	    print "\tsearching for enabled service=$service\n";
	    ($crc, $rc, $buf) = piperun($cmd, "egrep -e 'SERVICE[[:space:]]+$service' | awk '{print \$5, \$7, \$8}' | grep ENABLED | grep $slaves{$component} | head -n 1", "ubero");
	    print "\tfound rc=$rc, crc=$crc, buf=$buf\n";
	} else {
	    print "RUNNING: $cmd : $masters{$component} : $service\n";
	    system("$cmd");
	    ($crc, $rc, $buf) = piperun($cmd, "egrep -e 'SERVICE[[:space:]]+$service' | awk '{print \$5, \$7, \$8}' | grep $slaves{$component} | head -n 1", "ubero");
	    print "DONE: $crc $rc |$buf|\n";
	}
	if ($crc) {
	    print "\t$component $slaves{$component}: cannot determine status\n";
	} elsif ($rc || !$buf || $buf eq "") {
	    print "\t$component $slaves{$component}: is not enabled\n";
	} else {
	    if ($enabled_clc ne "") {
		print "WARN: both $component are marked as enabled\n";
	    } else {
		print "\t$component $slaves{$component}: is enabled\n";
		$enabled_clc = $slaves{$component};
	    }
	}
    }
    if ($enabled_clc eq "") {
	print "WARN: neither $component is marked as enabled\n";
	return(1);
    } else {
	if ($masters{$component} ne $enabled_clc) {
	    my $tmp = $slaves{$component};
	    $slaves{$component} = $masters{$component};
	    $masters{$component} = $tmp;
	}
    }
    return(0);
}

sub find_master_arbitrator {
    $cmd = "$runat $remote_pre euca_conf --list-arbitrators $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep ARBITRATOR | grep $masters{CLC} | head -n 1 | awk '{print \$2}'", "$ofile");
    if ($crc || $rc || !$buf  || $buf eq "") {
	doexit(1, "ERROR: cannot find arbitrator for master CLC ($masters{CLC}): ($crc, $rc, $buf)\n");
    }
    $current_artifacts{"master_arbitrator"} = $buf;
    $current_artifacts{"master_arbitrator_host"} = $masters{CLC};

    return(0);
}

sub find_arbitrator {
    my $component = shift @_;
    my $service = "arbitrator";

    print "\nLooking for arbitrator\n";
    open(FH, ">/tmp/ec2ops.out.$$");
    print FH "$current_artifacts{master_ds_buf}\n";
    close(FH);
    $cmd = "cat /tmp/ec2ops.out.$$";
    ($crc, $rc, $buf) = piperun($cmd, "egrep -e 'SERVICE[[:space:]]+$service' | awk '{print \$5, \$7, \$8}' | grep ENABLED | grep $component | head -n 1", "ubero");
    if ($crc) {
	print "\t$service $component: cannot determine status\n";
    } elsif ($rc || !$buf || $buf eq "") {
	print "\t$service $component: is not enabled\n";
    } else {
	print "\t$service $component: is enabled\n";
	unlink("/tmp/ec2ops.out.$$");
	return $component;
    }
    unlink("/tmp/ec2ops.out.$$");
    return "";
}

sub sync_qa_credentials {
    my $fromip = shift @_;
    my $toip = shift @_;

    print "\tsync_qa_credentials: FROM=$fromip TO=$toip\n";
    if ($fromip =~ /\d+\.\d+\.\d+\.\d+/ && $toip =~ /\d+\.\d+\.\d+\.\d+/) {
	my $cmd = "ssh -o StrictHostKeyChecking=no root\@$fromip 'if ( ! test -f /root/admin_cred.zip ); then $current_artifacts{eucahome}/usr/sbin/euca_conf --get-credentials /root/admin_cred.zip; fi'";
	run_command("$cmd", "no");
	my $cmd = "scp -o StrictHostKeyChecking=no root\@$fromip:/root/admin_cred.zip /tmp/admin_cred.zip.$$";
	run_command("$cmd", "no");
	my $cmd = "scp -o StrictHostKeyChecking=no /tmp/admin_cred.zip.$$ root\@$toip:/root/admin_cred.zip";
	run_command("$cmd", "no");
	my $cmd = "scp -o StrictHostKeyChecking=no root\@$toip:/root/admin_cred.zip /tmp/admin_cred.zip.$$";
	run_command("$cmd", "no");
	my $cmd = "scp -o StrictHostKeyChecking=no /tmp/admin_cred.zip.$$ root\@$fromip:/root/admin_cred.zip";
	run_command("$cmd", "no");

	my $cmd = "ssh -o StrictHostKeyChecking=no root\@$fromip unzip -o /root/admin_cred.zip";
	run_command("$cmd", "no");
	my $cmd = "ssh -o StrictHostKeyChecking=no root\@$toip unzip -o /root/admin_cred.zip";
	run_command("$cmd", "no");
    }
    return(0);
}

sub parse_input {
    print "PURGE UBERO BEGIN-----------------------\n";
    system ("rm -f ubero");
    print "PURGE UBERO COMPLETE-----------------------\n";
    print "BEGIN PARSING INPUT FILE\n-------------------\n";
    my $inputfile = "../input/2b_tested.lst";
    open(FH, "$inputfile") or die "failed to open $inputfile";
    while(<FH>) {
	chomp;
	my $line = $_;
	if ($line =~ /BZR_BRANCH\s+(.*)/) {
	    my $fullbranch = $1;
	    my (@tmp) = split("/", $fullbranch);
	    my $len = @tmp;
	    my $branch = $tmp[$len-1];
	    $current_artifacts{"branch"} = $branch;
	}
	my ($ip, $distro, $version, $arch, $source, @component_str) = split(/\s+/, $line);
	if ($ip =~ /\d+\.\d+\.\d+\.\d+/ && $distro && $version && ($arch eq "32" || $arch eq "64") && $source && @component_str) {
	    foreach $component (@component_str) {
		$component =~ s/\[//g;
		$component =~ s/\]//g;
		if ($masters{"$component"}) {
		    $slaves{"$component"} = "$ip";
		    $isha = 1;
		} else {
		    $masters{"$component"} = "$ip";
		}
		$roles{"$component"} = 1;

		if ($component =~ /^CLC(\d+)/ || $component =~ /^WS(\d+)/) {
		    $component =~ s/\d+//g;
		    if ($masters{"$component"}) {
			$slaves{"$component"} = "$ip";
			$isha = 1;
		    } else {
			$masters{"$component"} = "$ip";
		    }
		    $roles{"$component"} = 1;
		}
		if ($distro =~ /VMWARE/ && $component =~ /NC(\d+)/) {
		    $cc_has_broker{"CC$1"} = 1;
		}
		if ($component =~ /NC\d+/) {
		    if ($distro eq "FEDORA" || $distro eq "DEBIAN" || ($distro eq "RHEL" && $version =~ /^6\./) || ($distro eq "UBUNTU" && $source eq "REPO")) {
			$use_virtio = 1;
			setbfeimagelocation("http://192.168.7.65/bfebs-image/bfebs.img");
		    } elsif ($distro eq "UBUNTU") {
			setbfeimagelocation("http://192.168.7.65/bfebs-image/bfebs.img");
		    } else {
			setbfeimagelocation("http://192.168.7.65/bfebs-image/bfebs-xen.img");
		    }
		    
		}
	    }
	}
	# for setting windows images
	if ($line =~ /IMG_FILE=(.)+/){
             my @fields = split /\=/, $line;
	     $imgfile = $fields[1];
             $imgfile =~ s/\n|\r//;
	     $bfe_image="";
	     setbfeimagelocation("http://dmirror.eucalyptus/windows_images/$imgfile");	
             $trycount = 1080;
	}
    }
    close(FH);
    foreach $component (keys(%roles)) {
	print "Component (config file): $component Master: $masters{$component} Slave: $slaves{$component}\n";
    }

    my $this_mode = `cat ../input/2b_tested.lst | grep NETWORK`;
    chomp($this_mode);
    if( $this_mode =~ /^NETWORK\s+(\S+)/ ){
	my $mode = lc($1);
	if ($mode eq "system" || $mode eq "static") {
	    $ismanaged = 0;
	} else {
	    $ismanaged = 1;
	}
	$networkmode = $mode;
    }
    print "Network Mode: $networkmode\n";
    print "Use Virtio: $use_virtio\n";

    print "END PARSING INPUT FILE\n-------------------\n";

    if ($use_virtio) {
	$devname = "/dev/vdb";
	print "Using virtio device name: $devname\n";
    }
    
    if ($isha) {
	setremote($masters{CLC});
	sync_qa_credentials($masters{CLC}, $slaves{CLC});
	print "Synced /root/admin_cred.zip across CLCs\n";
	
	describe_services();
	find_real_master("CLC");
	describe_services();
	foreach $component (keys(%roles)) {
	    if ($component ne "CLC") {
		find_real_master($component);
	    }
	}
	foreach $component (keys(%roles)) {
	    print "Component (running system): $component Master: $masters{$component} Slave: $slaves{$component}\n";
	}

	$current_artifacts{"master_ds_buf"} = "";
	$current_artifacts{"slave_ds_buf"} = "";
    }
    return(0);
}

sub check_services_up {
    my @components = keys(%masters);
    if (@_) {
	@components = @_;
    }
    describe_services();
    foreach $key (@components) {
	my $i=0;
	print "\tcheck_services_up(): checking for component $key\n";
	$rc = 1;
	$rc = find_real_master($key);
	for ($i=0; $i<180 && $rc; $i++) {
	    print "\tcheck_services_up(): checking for component $key\n";
	    sync_qa_credentials($masters{CLC}, $slaves{CLC});
	    print "\tre-downloaded and re-synced /root/admin_cred.zip across CLCs\n";
	    describe_services();
	    $rc = find_real_master($key);
	}
	if ($rc) {
	    doexit(1, "ERROR: no master found for component $key");
	}
    }
    return(0);
}

sub discover_test_state {
    my $testkey = shift @_ || "ec2ops";
    my $instance, $group, $key, $ret=0;

    $cmd = "$runat $remote_pre $prefix-describe-keypairs $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | tail -n 1 | awk '{print \$2}'", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	$key = $buf;
	print "DISCOVERED KEY: $key\n";
    }

    $cmd = "$runat $remote_pre $prefix-describe-groups $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | tail -n 1 | awk '{print \$3}'", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	$group = $buf;
	print "DISCOVERED GROUP: $group\n";
    }

    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | grep INST | grep -v erminated | grep -v hutting | tail -n 1", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	($meh, $instance, $meh, $publicip, $privateip, $state, @meh) = split(/\s+/, $buf);
	print "BUF: |$buf|\n";
	if ($instance =~ /i-.*/ && $publicip =~ /\d+\.\d+\.\d+\.\d+/ && $privateip =~ /\d+\.\d+\.\d+\.\d+/ && $state ne "") {
	    print "DISCOVERED INSTANCE: $instance, $publicip, $privateip, $state\n";
	} else {
	    $ret = 1;
	}
    }

    if (!$ret) {
	$current_artifacts{"keypair"} = "$key";
	$current_artifacts{"keypairfile"} = "$keypath/$key" . ".priv";
	$current_artifacts{"group"} = "$group";
	$current_artifacts{"instance"} = "$instance";
	$current_artifacts{"instances"} .= "$instance ";
	$current_artifacts{"instanceip"} = "$publicip";
	$current_artifacts{"instanceprivateip"} = "$privateip";
	$current_artifacts{"instancestate"} = "$state";
	$current_artifacts{"instancestates"} .= "$state ";
	
	$cleanup_artifacts{"groups"} .= "$group ";
	$cleanup_artifacts{"instances"} .= "$instance ";
	$cleanup_artifacts{"keypairs"} .= "$key ";
	$cleanup_artifacts{"keypairfiles"} .= "$keypath/$key" . ".priv ";
    }

    
    return($ret);
}

sub run_walrus_upload {
    my $cmd = "$runat $remote_pre euca-bundle-image -i /etc/hosts -d /tmp/ $remote_post";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAIL: count not run cmd '$cmd'\n");
    }

    my $cmd = "$runat $remote_pre euca-upload-bundle -b hadnsbucket -m /tmp/hosts.manifest.xml $remote_post";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAIL: count not run cmd '$cmd'\n");
    }

    return(0);
}

sub run_ec2_describes {
    my @ops = ('availability-zones', 'images', 'addresses', 'images', 'instances', 'keypairs', , 'snapshots', 'volumes');

    foreach $op (@ops) {
	$cmd = "$runat $remote_pre $prefix-describe-$op $remote_post";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAIL: count not run cmd '$cmd'\n");
	}
    }
    if ($prefix eq "euca") {
	$cmd = "$runat $remote_pre $prefix-describe-groups $remote_post";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAIL: count not run cmd '$cmd'\n");
	}
    } elsif ($prefix eq "ec2") {
	$cmd = "$runat $remote_pre $prefix-describe-group $remote_post";
	my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAIL: count not run cmd '$cmd'\n");
	}
    }
    
    return(0);
}

sub discover_static_info {
    discover_emis();
    print "SUCCESS: discovered loaded image: current=$current_artifacts{instancestoreemi}, all=$static_artifacts{instancestoreemis}\n";

    discover_zones();
    print "SUCCESS: discovered available zone: current=$current_artifacts{availabilityzone}, all=$static_artifacts{availabilityzones}\n";

    discover_vmtypes();
    print "SUCCESS: discovered vmtypes: m1smallmax=$static_artifacts{m1smallmax} m1smallavail=$static_artifacts{m1smallavail}\n";
    
    return(0);
}

sub discover_vmtypes {

    if ($ismanaged) {
	$cmd = "$runat $remote_pre $prefix-describe-addresses $remote_post";
	($crc, $rc, $addrcount) = piperun($cmd, "awk 'BEGIN{c=0}/nobody/{c++}END{print c}'", "ubero");
	$addrcount = int($addrcount);
	if ($rc || $addrcount < 0) {
	    doexit(1, "FAILED: ($crc, $rc, $addrcount) = $cmd\n");
	}
    } else {
	$addrcount = 9999999;
    }

    if ($static_artifacts{"availabilityzones"}) {
	my @zones = split(/\s+/, $static_artifacts{"availabilityzones"});
	foreach $zone (@zones) {
	    my $cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
	    my ($crc, $rc, $m1scount) = piperun($cmd, "grep $zone -A6 | grep m1.small | awk '{print \$4}' | head -n 1", "ubero");
	    $m1scount = int($m1scount);
	    if ($rc || $m1scount < 0) {
		doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
	    }
	    my $key = $zone . "m1smallavail";
	    $static_artifacts{"$key"} = $m1scount > $addrcount ? $addrcount : $m1scount;
	    
	    my $cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
	    my ($crc, $rc, $m1scount) = piperun($cmd, "grep $zone -A6 | grep m1.small | awk '{print \$6}' | head -n 1", "ubero");
	    $m1scount = int($m1scount);
	    if ($rc || $m1scount < 0) {
		doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
	    }
	    my $key = $zone . "m1smallmax";
	    $static_artifacts{"$key"} = $m1scount > $addrcount ? $addrcount : $m1scount;
	}
    }
    $cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
    ($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$4}' | tail -n 1", "ubero");
    $m1scount = int($m1scount);
    if ($rc || $m1scount < 0) {
	doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
    }
    $static_artifacts{m1smallavail} = $m1scount > $addrcount ? $addrcount : $m1scount;
    
    ($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$6}' | tail -n 1", "ubero");
    $m1scount = int($m1scount);
    if ($rc || $m1scount < 0) {
	doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
    }
    $static_artifacts{m1smallmax} = $m1scount > $addrcount ? $addrcount : $m1scount;

    return(0);
}

sub register_snapshot {
    $snap = $current_artifacts{"snapshot"};

    if ( ! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }
    if ( $bfe_image =~ /windows.*/){
	    $cmd = "$runat $remote_pre $prefix-register -n windows_image --kernel windows --root-device-name /dev/sda1 -b /dev/sda1=$snap $remote_post";
    }else{
            $cmd = "$runat $remote_pre $prefix-register -n testImage --root-device-name /dev/sda1 -b /dev/sda1=$snap $remote_post";
    }
#    $cmd = "$prefix-register -n testImage --root-device-name /dev/sda1 -b /dev/sda1=$snap";
    ($crc, $rc, $emi) = piperun($cmd, "grep IMAGE | awk '{print \$2}'", "ubero");
    if ($rc || !$emi || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	doexit(1, "FAILED: $cmd\n");
    }

    $cleanup_artifacts{"emis"} .= "$emi ";
    $current_artifacts{"ebsemi"} = "$emi";

    return(0);
}

sub authorize_ssh {
    return(authorize_ssh_from_cidr(@_));
}

sub authorize_ssh_from_cidr {
    my $group = shift @_ || $current_artifacts{group} || "default";
    my $cidr = shift @_ || "0.0.0.0/0";

    if (! $group || $group eq "") {
	doexit(1, "FAILED: invalid group '$group'\n");
    }
    
    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-authorize $group -P tcp -p 22 -s $cidr $remote_post";
    
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }

    $cleanup_artifacts{"rules"} .= "$group -P tcp -p 22 -s $cidr,";
    
    return(0);
}

sub find_instance_volume {
    $keypairfile = $current_artifacts{keypairfile} || shift @_;
    $instanceip = $current_artifacts{instanceip} || shift @_;
    if (! -f "$keypairfile") {
	doexit(1, "ERROR: cannot find keypairfile '$keypairfile'\n");
    }
    if ( ! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/ )) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }

    sleep($library_sleep);

    if ($use_virtio) {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'ls /dev/vd\* | tail -n 1 | grep -v -e [0-9]'";
    } else {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'ls /dev/sd\* | grep -v sda | tail -n 1 | grep -v -e [0-9]'";
    }
    $done=0; 
    my $i;
    for ($i=0; $i<60 && !$done; $i++) {
	($crc, $rc, $buf) = piperun($cmd, "grep -v RUNAT | grep dev | tail -n 1", "$ofile");
	if ($rc || ! ($buf =~ /^\/dev\/.*/) ) {
	    print "\twaiting for dev to appear...\n";
	    sleep(1);
	} else {
	    $done++;
	}
    }
    if ($rc || ! ($buf =~ /^\/dev\/.*/) ) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }
    $current_artifacts{instancedevice} = $buf;
    
    return(0);
}

sub run_command {
    $icmd = shift @_ || "echo HELLO WORLD";
    $failstop = shift @_ || "yes";

    sleep($library_sleep);
    
    $cmd = "$runat $icmd";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($failstop eq "yes" && ($crc || $rc)) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub run_command_not {
    $icmd = shift @_ || "echo HELLO WORLD";
    
    sleep($library_sleep);
    
    $cmd = "$runat $icmd";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if (!$crc || $rc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub ping_instance_from_cc {
    $instanceip = shift @_ || $current_artifacts{instanceprivateip};
    $doexit = shift @_ || "y";

    my @ccips, $ccidx=0, $key;

    foreach $key (keys(%masters)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $masters{$key};
	    $ccidx++;
	}
    }
    foreach $key (keys(%slaves)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $slaves{$key};
	    $ccidx++;
	}
    }
    
    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }

    if ($bfe_image =~ /windows.*/){
          if($imgfile =~ /2008.*/ or $imgfile =~ /windows7.*/){
		print "passing PING tests on newer windows versions ($imgfile)\n";
		return(0);
          }
    }
    
    sleep($library_sleep);
    
    my $i=0, $j=0;
    my $done=0;
    for ($i=0; $i<$pingtries && !$done; $i++) {
	for ($j=0; $j<$ccidx && !$done; $j++) {
	    $ccip = $ccips[$j];
	    if ($ccip =~ /\d+\.\d+\.\d+\.\d+/) {
		$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'ping -c 1 -w 1 $instanceip'";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if (!$rc && !$crc) {
		    $current_artifacts{instancecc} = $ccip;
		    $done++;
		} else {
		    print "\twaiting to be able to ping instance ($instanceip) from cc ($ccip): ($i/$pingtries)\n";
		}
	    }
	}
	if (!$done) {
	    sleep(1);
	}
    }
    if (!$done) {
	if ($doexit eq "y") {
	    doexit(1, "FAILED: could not ping instance ($instanceip) from cc ($ccip)\n");
	} else {
	    print "\tWARN: couldn't ping instance ($instanceip) from cc ($ccip), skipping\n";
	}
    }

    return(0);

}

sub run_instance_command {
    $icmd = shift @_ || "echo HELLO WORLD";
    $keypairfile = shift @_ || $current_artifacts{keypairfile};
    $instanceip = shift @_ || $current_artifacts{instanceip};

    if (! -f "$keypairfile" ) {
	doexit(1, "ERROR: cannot find keypairfile '$keypairfile'\n");
    }
    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }
    
    sleep($library_sleep);

    my $i=0;
    my $done=0;
    for ($i=0; $i<30 && !$done; $i++) {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'echo HELLO WORLD'";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if (!$rc && !$crc) {
	    $done++;
	} else {
	    print "\twaiting to be able to ssh to instance ($i/30)\n";
	    sleep(1);
	}
    }    
    if (!$done) {
	doexit(1, "FAILED: could not ssh to instance\n");
    }
    
    $cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip '$icmd'";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
	doexit(1, "ERROR: running '$cmd' = ($crc, $rc)\n");
    }

    return(0);
}

sub copy_to_instance {
    $file = shift @_ || "echo HELLO WORLD";
    $keypairfile = shift @_ || $current_artifacts{keypairfile};
    $instanceip = shift @_ || $current_artifacts{instanceip};

    if (! -f "$keypairfile" ) {
	doexit(1, "ERROR: cannot find keypairfile '$keypairfile'\n");
    }
    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }
    
    sleep($library_sleep);

    my $i=0;
    my $done=0;
    for ($i=0; $i<30 && !$done; $i++) {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'echo HELLO WORLD'";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if (!$rc && !$crc) {
	    $done++;
	} else {
	    print "\twaiting to be able to ssh to instance ($i/30)\n";
	    sleep(1);
	}
    }    
    if (!$done) {
	doexit(1, "FAILED: could not ssh to instance\n");
    }
    
    $cmd = "$runat scp -o StrictHostKeyChecking=no -i $keypairfile $file root\@$instanceip:";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
	doexit(1, "ERROR: running '$cmd' = ($crc, $rc)\n");
    }

    return(0);
}

sub setemitype {
    $emitype = shift @_ || "instancestoreemi";
    return(0);
}

sub run_instances {
    my $num = shift @_ || 1;

    my $emi = $current_artifacts{$emitype};
    my $keypair = $current_artifacts{keypair};
    my $zone = $current_artifacts{availabilityzone};
    my $group = $current_artifacts{group};
    my $type = "m1.small";
    if ($bfe_image =~ /windows.*/){
	$type = "m1.xlarge";
    }

#    $zone = "";

    if (! ($emi =~ /emi-.*/) ) {
	doexit(1, "ERROR: invalid emi '$emi'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-run-instances -n $num $emi";
    if ($keypair) {
	$cmd .= " -k $keypair";
    } 
    if ($zone) {
	$cmd .= " -z $zone";
    }
    if ($group) {
	$cmd .= " -g $group";
    }
    if ($type) {
	$cmd .= " -t $type";
    }
    
    $cmd .= " $remote_post";

    ($crc, $rc, $buf) = piperun($cmd, "grep INSTANCE | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }
    
    my @insts = split(/\s+/, $buf);
    foreach $inst (@insts) {
	if (!$inst || $inst eq "" || !($inst =~ /i-.*/)) {
	    print "WARN: insts=@insts, inst=$inst\n";
	} else {
	    $cleanup_artifacts{instances} .= "$inst ";
	    $current_artifacts{instance} = $inst;
	    $current_artifacts{instances} .= "$inst ";
	    $current_artifacts{"instances"} =~ s/\s+/ /g;
	    $current_artifacts{"instances"} =~ s/^\s+//g;
	}
    }
    if (!$current_artifacts{instance}) {
	doexit(1, "FAILED: could not run instance\n");
    }
    $current_artifacts{instancestate} = "pending";
    $current_artifacts{instancestates} .= "pending ";

    return(0);
}

sub add_group {
    my $ingroup = shift @_ || "mygroup";
    
    if ($ingroup eq "default") {
	$current_artifacts{"group"} = "$ingroup";
	return(0);
    }
    
    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-add-group $ingroup -d '$ingroup' $remote_post";
    ($crc, $rc, $group) = piperun($cmd, "", "$ofile");
    if ($rc || !$group || $group eq "") {
	doexit(1, "FAILED: no group\n");
    }
    
    $cleanup_artifacts{"groups"} .= "$ingroup ";    
    $current_artifacts{"group"} = "$ingroup";
    if (!$current_artifacts{group}) {
	doexit(1, "FAILED: could not add group\n");
    }

    return(0);
}

sub transfer_credentials {
    my $fromip = shift @_ || $masters{"CLC"};    
    my $toip = shift @_ || $masters{"CLC"};    

    my $cmd = "scp -o StrictHostKeyChecking=no root\@$fromip:/root/eucalyptus-admin-qa.zip /tmp/eucalyptus-admin-qa.zip.$$";
    run_command("$cmd");

    my $cmd = "scp -o StrictHostKeyChecking=no /tmp/eucalyptus-admin-qa.zip.$$ root\@$toip:/root/eucalyptus-admin-qa.zip";
    run_command("$cmd");

    my $cmd = "ssh -o StrictHostKeyChecking=no root\@$toip unzip -o /root/eucalyptus-admin-qa.zip -d /root/eucalyptus-admin-qa/";
    run_command("$cmd");

    unlink("/tmp/eucalyptus-admin-qa.zip.$$");

    return(0);
}

sub source_credentials {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";

    $cmd = "$runat $remote_pre ls -l /root/$account-$user-qa/eucarc $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    
    $current_artifacts{"account"} = $account;
    $current_artifacts{"user"} = $user;
    
    setremote();
    
    return(0);
}

sub get_credentials {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";

    sleep($library_sleep);
    
    $cmd = "$runat $remote_pre rm -f /root/$account-$user-qa.zip $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }

    $oldrunat = $runat;
    setrunat("runat 120");
    $cmd = "$runat $remote_pre euca-get-credentials -a $account -u $user /root/$account-$user-qa.zip $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    setrunat("$oldrunat");

    $cmd = "$runat $remote_pre unzip -o /root/$account-$user-qa.zip -d /root/$account-$user-qa/ $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    
    return(0);
}

sub grant_allpolicy {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";
    my $doaccount=0, $douser=0;

    if ($user eq "admin") {
	return(0);
    }

    sleep($library_sleep);

    open(OFH, ">/tmp/thepolicy.$$");
    my $thepolicy = '{
 "Version":"2011-04-01",
 "Statement":[{
   "Sid":"1",
   "Effect":"Allow",
   "Action":"*",
   "Resource":"*"
 }]
}';
    print OFH "$thepolicy";
    close(OFH);

    run_command("scp -o StrictHostKeyChecking=no /tmp/thepolicy.$$ root\@$current_artifacts{remoteip}:/tmp/thepolicy");

    $cmd = "$runat $remote_pre euare-useruploadpolicy --delegate=$account -u $user -p allpolicy -f /tmp/thepolicy $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }

    return(0);
}

sub create_account_and_user {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";
    my $doaccount=0, $douser=0;

    if ($account eq "eucalyptus") {
	$current_artifacts{"account"} = "$account";
    } else {
	$doaccount=1;
    }
    if ($user eq "admin") {
	$current_artifacts{"user"} = "$user";
    } else {
	$douser=1;
    }
    
    sleep($library_sleep);

    if ($doaccount) {
	$cmd = "$runat $remote_pre euare-accountcreate -a $account $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "tail -n 1 | grep $account | awk '{print \$1}'", "$ofile");
	if ($rc || $buf ne "$account") {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	
	$cleanup_artifacts{"accounts"} .= "$account ";    
	$current_artifacts{"account"} = "$account";
	if (!$current_artifacts{account}) {
	    doexit(1, "FAILED: could not add account\n");
	}
    }
    if ($douser) {
	$cmd = "$runat $remote_pre euare-usercreate --delegate=$account -u $user $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	
	$cleanup_artifacts{"users"} .= "$account/$user ";    
	$current_artifacts{"user"} = "$user";
	if (!$current_artifacts{user}) {
	    doexit(1, "FAILED: could not add user\n");
	}
    }

    return(0);
}

sub add_keypair {
    my $inkey = shift @_ || "mykey";

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-add-keypair $inkey $remote_post";
    ($crc, $rc, $key) = piperun($cmd, "", "$ofile");
    if ($crc || $rc || !$key || $key eq "") {
	doexit(1, "FAILED: no key\n");
    }
    open(FH, "> $keypath/$inkey.priv") || doexit(1, "ERROR: could not write to $keypath/$inkey.priv");
    print FH "$key";
    close(FH);
    system("chmod 0600 $keypath/$inkey.priv");
    
    $cleanup_artifacts{"keypairs"} .= "$inkey ";    
    $cleanup_artifacts{"keypairfiles"} .= "$keypath/$inkey" . ".priv ";    
    $current_artifacts{"keypair"} = "$inkey";
    $current_artifacts{"keypairfile"} = "$keypath/$inkey" . ".priv";
    if (!$current_artifacts{keypair}) {
	doexit(1, "FAILED: could not add keypair\n");
    }
    
    return(0);
}

sub attach_volume {
    my $remote = $devname;
    my $inst = $current_artifacts{instance};
    my $vol = $current_artifacts{volume};
    
    if ( ! ($remote =~ /\/dev.*/) ) {
	doexit(1, "ERROR: invalid remote dev name '$devname'\n");
    }

    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }
    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-attach-volume $vol -i $inst -d $remote $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not attach volume\n");
    }
    $current_artifacts{volumestate} = "attaching";
    
    return(0);
}

sub detach_volume {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-detach-volume $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not detach volume\n");
    }
    $current_artifacts{volumestate} = "available";
    
    return(0);
}


sub delete_volume {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-delete-volume $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not delete volume\n");
    }
    $current_artifacts{volumestate} = "deleted";
    
    return(0);
}

sub delete_snapshot {
    my $snap = $current_artifacts{snapshot};

    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-delete-snapshot $snap $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep SNAPSHOT | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "snap-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    $current_artifacts{snapshotstate} = "deleted";
    
    return(0);
}

sub create_volume {
    my $size = shift @_ || 1;
    if ($bfe_image =~ /windows.*/){
	    $size=15;
            $remote_cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$current_artifacts{remoteip} 'source /root/eucarc; $current_artifacts{eucahome}/usr/sbin/euca-modify-property -p $current_artifacts{availabilityzone}.storage.maxvolumesizeingb=$size > /dev/null 2>&1'"; 
            ($crc, $rc, $buf) = piperun($remote_cmd, "", "ubero");  
	    if($rc){	
	         doexit(1, "FAILED: ($rc, $buf) = $remote_cmd\n");
            }

            $remote_cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$current_artifacts{remoteip} 'source /root/eucarc; $current_artifacts{eucahome}/usr/sbin/euca-modify-property -p $current_artifacts{availabilityzone}.storage.shouldtransfersnapshots=false > /dev/null 2>&1'";
            ($crc, $rc, $buf) = piperun($remote_cmd, "", "ubero");
            if($rc){
                 doexit(1, "FAILED: ($rc, $buf) = $remote_cmd\n");
            }
    }

    my $zone = $current_artifacts{availabilityzone};

    if (! $zone ) {
	doexit(1, "ERROR: invalid zone '$zone'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-volume -z $zone -s $size $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    save_volid("$buf");
    $cleanup_artifacts{"volumes"} .= "$buf ";    
    $current_artifacts{"volume"} = $buf;
    $current_artifacts{"volumestate"} = "UNSET";
    if (!$current_artifacts{"volume"}) {
	doexit(1, "FAILED: could not create volume\n");
    }
    
    return(0);
}


sub create_snapshot_volume {
    my $snap = $current_artifacts{snapshot};
    my $zone = $current_artifacts{availabilityzone};

    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }
    if (! $zone ) {
	doexit(1, "ERROR: invalid zone ID '$zone'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-volume --snapshot $snap -z $zone $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    save_volid("$buf");
    $cleanup_artifacts{"volumes"} .= "$buf ";    
    $current_artifacts{"volume"} = $buf;
    $current_artifacts{"volumestate"} = "UNSET";
    if (!$current_artifacts{"volume"}) {
	doexit(1, "FAILED: could not create volume\n");
    }
    
    return(0);
}

sub create_snapshot {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-snapshot $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep SNAPSHOT | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "snap-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }

    $cleanup_artifacts{"snapshots"} .= "$buf ";    
    $current_artifacts{"snapshot"} = $buf;
    $current_artifacts{"snapshotstate"} = "pending";
    
    return(0);
}

sub create_image {
    $instance = $current_artifacts{"instance"};

    if ( ! ($instance =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$instance'\n");
    }
    
    $cmd = "$runat $remote_pre $prefix-create-image -n testImage $instance $remote_post";
    ($crc, $rc, $emi) = piperun($cmd, "grep IMAGE | awk '{print \$2}'", "ubero");
    if ($rc || !$emi || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	doexit(1, "FAILED: $cmd\n");
    }

    $cleanup_artifacts{"emis"} .= "$emi ";
    $current_artifacts{"ebsemi"} = "$emi";

    return(0);
}

sub discover_keypair {
    sleep($library_sleep);

    my $done=0;
    my $i;
    my $keypair;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-keypairs $remote_post";
	($crc, $rc, $keypair) = piperun($cmd, "grep KEYPAIR | awk '{print \$2}' | tail -n 1", "$ofile");
	print "\tKEYPAIR: $inst\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $keypair) = $cmd\n");
	} 
	$done++;
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance\n");
    }
    $current_artifacts{keypair} = $keypair;
    $current_artifacts{keypairfile} = "$keypath/$keypair.priv";
    return(0);
}


sub discover_instance {
    sleep($library_sleep);

    my $done=0;
    my $i;
    my $inst;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
	($crc, $rc, $inst) = piperun($cmd, "grep INSTANCE | awk '{print \$2}' | tail -n 1", "$ofile");
	print "\tINSTANCE: $inst\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
	} elsif (! ($inst =~ /i-.*/) ) {
            doexit(1, "ERROR: invalid instance ID '$inst'\n");
        }
	$done++;
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance\n");
    }
    $current_artifacts{instance} = $inst;

    return(0);
}

sub wait_for_instance {
    my $inst = shift @_ || $current_artifacts{instance};

    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$instance_run_timeout && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep INSTANCE | awk '{print \$6}' | tail -n 1", "$ofile");
	print "\t($i/$instance_run_timeout) STATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state eq "running") {
	    $done++;
	} elsif ($state eq "shutting-down" || $state eq "terminated") {
	    doexit(1, "FAILED: waiting for instance to run (state went to $state)\n");
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance to run\n");
    }
    $current_artifacts{instancestate} = "running";
    if ( $bfe_image =~ /windows.*/){
        sleep(360); # slack for booting windows
    }

    return(0);
}

sub wait_for_instance_ip {
    return(wait_for_instance_ip_public(@_));
}

sub wait_for_instance_ip_public {
    my $inst = shift @_ || $current_artifacts{instance};
    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $ip) = piperun($cmd, "grep INSTANCE | awk '{print \$4}' | tail -n 1", "$ofile");
	print "\tIP: $ip\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $ip) = $cmd\n");
	} elsif ($ip && $ip ne "0.0.0.0") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance public ip\n");
    }
    $current_artifacts{instanceip} = "$ip";

    return(0);
}

sub wait_for_instance_ip_private {
    my $inst = shift @_ || $current_artifacts{instance};
    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $ip) = piperun($cmd, "grep INSTANCE | awk '{print \$5}' | tail -n 1", "$ofile");
	print "\tIP: $ip\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $ip) = $cmd\n");
	} elsif ($ip && $ip ne "0.0.0.0") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance private ip\n");
    }
    $current_artifacts{instanceprivateip} = "$ip";

    return(0);
}

sub discover_volume {
    my $done=0;

    sleep($library_sleep);
    my $i;
    my $vol;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $remote_post";
	($crc, $rc, $vol) = piperun($cmd, "grep VOLUME | awk '{print \$2}' | tail -n 1", "$ofile");
	print "\tVOLUME: $vol\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $vol) = $cmd\n");
	} elsif (! ($vol =~ /vol-.*/) ) {
            doexit(1, "ERROR: invalid volume ID '$vol'\n");
        }
	$done++;
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume\n");
    }
    $current_artifacts{volume} = $vol;

    return(0);
}

sub wait_for_volume_attach {
    my $vol = shift @_ || $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /in-use/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be attached\n");
    }
    $current_artifacts{volumestate} = "in-use";

    return(0);
}


sub wait_for_volume {
    my $vol = shift @_ || $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /available/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be available\n");
    }
    $current_artifacts{volumestate} = "available";

    return(0);
}

sub wait_for_snapshot {
    my $snap = shift @_ || $current_artifacts{snapshot};
    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-snapshots $snap $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep SNAPSHOT | awk '{print \$4}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state eq "completed") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for snapshot to be completed\n");
    }
    $current_artifacts{snapshotstate} = "completed";

    return(0);
}

sub wait_for_volume_detach {
    my $vol = shift @_ || $current_artifacts{volume};
    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /available/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be detached\n");
    }
    $current_artifacts{volumestate} = "available";

    return(0);
}

sub discover_zones {
    my $zone, $buf;

    $static_artifacts{availabilityzones} = "";
    $current_artifacts{availabilityzone} = "";
    sleep($library_sleep);
    $cmd = "$runat $remote_pre $prefix-describe-availability-zones $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep AVAILABILITYZONE | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: no zone\n");
    }
    my @zones = split(/\s+/, $buf);
    my $zonecount=0;
    foreach $zone (@zones) {
	if (!$zone || $zone eq "") {
	    print "WARN: zones=@zones, zone=$zone\n";
	} else {
	    $static_artifacts{"availabilityzones"} .= "$zone ";
	    $current_artifacts{"availabilityzone"} = $zone;
	    $zonecount++;
	}
    }
    $current_artifacts{"availabilityzonecount"} = $zonecount;

    return(0);
}

sub discover_emis {
    my $emi, $buf;

    sleep($library_sleep);
    $cmd = "$runat $remote_pre $prefix-describe-images $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep IMAGE | grep -v 'deregister' | grep -v ebs | grep -i 'mi-' | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    my @emis = split(/\s+/, $buf);
    foreach $emi (@emis) {
	if (!$emi  || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	    print "WARN: emis=@emis, emi=$emi\n";
	} else {
	    $static_artifacts{"instancestoreemis"} .= "$emi ";
	    $current_artifacts{"instancestoreemi"} = $emi;
	}
    }

    return(0);
}

sub terminate_instances {
    my $num = shift @_ || 1;
    my $count=0;
    my @insts = split(/\s+/, $current_artifacts{"instances"});
    foreach $inst (@insts) {
	$cmd = "$runat $prefix-terminate-instances $inst";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	$current_artifacts{"instances"} =~ s/$inst//g;
	$current_artifacts{"instances"} =~ s/\s+/ /g;
	$current_artifacts{"instances"} =~ s/^\s+//g;
	$count++;
	if ($count >= $num) {
	    return(0);
	}
    }
    return(0);
}

sub setpiperetries {
    my $retries = shift @_ || 1;
    if ($retries > 0) {
	$piperetries = $retries;
    }
    return(0);
}

sub piperun {
    my $cmd = shift @_;
    my $pipe = shift @_;
    my $uberofile = shift @_ || "/tmp/uberofile.$$";
    my $retries = shift @_ || $piperetries;
    my $pipestr = "";
    my $buf="";
    my $rc;

    if ($pipe) {
	$pipestr = "| $pipe";
    }
    
    my $done=0;
    for (my $i=0; $i<$retries && !$done; $i++) {
	system("$cmd > /tmp/tout.$$ 2>&1");
	$retcode = ${^CHILD_ERROR_NATIVE};
	
	chomp($buf = `cat /tmp/tout.$$ $pipestr`);
	$pipecode = system("cat /tmp/tout.$$ $pipestr >/dev/null 2>&1");
	
	system("echo '*****' >> $uberofile");
	system("echo CMD=$cmd >> $uberofile");
	$rc = system("cat /tmp/tout.$$ >> $uberofile");
	unlink("/tmp/tout.$$");
	sleep(1);

	# catch the 'ssh to remote host didn't work' exit code
	if ($retcode != 255) {
	    $done++;
	}
    }
    return($retcode, $pipecode, $buf);
}

sub wait_for_instance_death {
    my $inst = shift @_;
    my $exitmode = shift @_;
    my $exitbadly = 1;
    if ($exitmode eq "warn") {
	$exitbadly = 0;
    }

    if (! ($inst =~ /i-.*/) ) {
	if ($exitbadly) {
	    doexit(1, "ERROR: invalid instance ID '$inst'\n");
	} else {
	    print "WARN: invalid instance ID\n";
	    return(1);
	}
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$instance_run_timeout && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep INSTANCE | awk '{print \$6}' | tail -n 1", "$ofile");
	print "\t($i/$instance_run_timeout) STATE: $state\n";
	if ($rc) {
	    if ($exitbadly) {
		doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	    } else {
		print "WARN: terminate command failed ($rc, $state) = $cmd\n";
	    }
	} elsif ($state eq "terminated" || $state eq "") {
	    $done++;
	} elsif ($state eq "running") {
	    if ($exitbadly) {
		doexit(1, "FAILED: waiting for instance to terminate (state is $state)\n");
	    } else {
		print "WARN: waiting for instance to terminate but state is running\n";
	    }
	}
    }
    if (!$done) {
	if ($exitbadly) {
	    doexit(1, "FAILED: waiting for instance to terminate\n");
	} else {
	    print "WARN: timedout waiting for instance to terminate\n";
	}
    }

    return(0);
}

sub docleanup {
    my $failures=0;

    print "Instances\n\t";
    my @insts = split(/\s+/, $cleanup_artifacts{"instances"});
    my $numinsts = @insts;
    if ($numinsts) {
	foreach $inst (@insts) {
	    if ($inst) {
		print "$inst";
		$cmd = "$runat $remote_pre $prefix-terminate-instances $inst $remote_post";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    }
	}
	sleep(30);
	foreach $inst (split(/\s+/, $cleanup_artifacts{"instances"})) {
            print "\nWaiting for instance $inst to terminate...\n";
            wait_for_instance_death($inst, "warn");
	    if ($inst) {
		print "$inst";
		$cmd = "$runat $remote_pre $prefix-terminate-instances $inst $remote_post";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
		$cleanup_artifacts{"instances"} =~ s/$inst//g;
	    }
	}
    }
    print "\n";

    
    print "Images\n\t";
    foreach $emi (split(/\s+/, $cleanup_artifacts{"emis"})) {
	if ($emi) {
	    print "$emi";
	    $cmd = "$runat $remote_pre $prefix-deregister $emi $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cmd = "$runat $remote_pre $prefix-deregister $emi $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"emis"} =~ s/$emi//g;
	}
    }
    print "\n";
    
    print "Volumes\n\t";
    # snoop for bfebs volumes left behind
    if ($current_artifacts{snapshot}) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep $current_artifacts{snapshot} | awk '{print \$2}'", "$ofile");
	if (!$crc && !$rc && $buf ne "") {
	    $buf =~ s/\s+/ /g;
	    $cleanup_artifacts{"volumes"} .= "$buf";
	}
    }
    foreach $vol (split(/\s+/, $cleanup_artifacts{"volumes"})) {	
	if ($vol) {
	    print "$vol";
	    save_volid("$vol");
	    $cmd = "$runat $remote_pre $prefix-delete-volume $vol $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"volumes"} =~ s/$vol//g;
	}
    }
    print "\n";
    
    print "Snapshots\n\t";
    foreach $snap (split(/\s+/, $cleanup_artifacts{"snapshots"})) {
	if ($snap) {
	    print "$snap";
	    $cmd = "$runat $remote_pre $prefix-delete-snapshot $snap $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"snapshots"} =~ s/$snap//g;
	}
    }
    print "\n";
    
    print "Rules\n\t";
    foreach $rule (split(",", $cleanup_artifacts{"rules"})) {
	if ($rule) {
	    print "$rule";
	    $cmd = "$runat $remote_pre $prefix-revoke $rule $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"rules"} =~ s/$rule//g;
	}
    }
    print "\n";
    
    print "Groups\n\t";
    foreach $group (split(/\s+/, $cleanup_artifacts{"groups"})) {
	if ($group) {
	    print "$group";
	    $cmd = "$runat $remote_pre $prefix-delete-group $group $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"groups"} =~ s/$group//g;
	}
    }
    print "\n";
    
    print "Keypairs\n\t";
    foreach $key (split(/\s+/, $cleanup_artifacts{"keypairs"})) {
	if ($key) {
	    print "$key";
	    $cmd = "$runat $remote_pre $prefix-delete-keypair $key $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"keypairs"} =~ s/$key//g;
	}
    }
    print "\n";
    
    print "Keypairfiles\n\t";
    foreach $keyf (split(/\s+/, $cleanup_artifacts{"keypairfiles"})) {
	if ($keyf) {
	    if ( -f "./$keyf" ) {
		print "$keyf";
		$cmd = "rm -f ./$keyf";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
		$cleanup_artifacts{"keypairfiles"} =~ s/$keyf//g;
	    }
	}
    }
    print "\n";
    
    $current_artifacts{account} = "";
    $current_artifacts{user} = "";
    
    print "Users\n\t";
    foreach $acuser (split(/\s+/, $cleanup_artifacts{"users"})) {
	if ($acuser) {
	    setremote($current_artifacts{remoteip});
	    my ($account, $user) = split("/", $acuser);
	    print "$account/$user";
	    $cmd = "$runat $remote_pre euare-userdel --delegate=$account -R -u $user $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"users"} =~ s/$acuser//g;
	}
    }
    print "\n";
    
    print "Accounts\n\t";
    foreach $account (split(/\s+/, $cleanup_artifacts{"accounts"})) {
	if ($account) {
	    setremote($current_artifacts{remoteip});
	    print "$account";
	    $cmd = "$runat $remote_pre euare-accountdel -r -a $account $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; $failures++; } else { print "(success) "; }
	    $cleanup_artifacts{"accounts"} =~ s/$account//g;
	}
    }
    print "\n";
    return($failures);
}

sub doexit {
    my $code = shift @_;
    my $msg = shift @_;

    if ($msg) {
	print "$msg";
    }
    
    use_euca2ools();

    print_all_metadata();

    if ($cleanup || $code) {
	print "BEGIN CLEANING UP TEST ARTIFACTS\n----------------\n";

	my $fails = docleanup();

	print "END CLEANING UP (cleanup failures=$fails)\n---------------\n";
    }

    if ( -f "$ofile" ) {
	print "BEGIN OUTPUT TRACE\n------------\n";
	system("cat $ofile");
	print "END OUTPUT TRACE\n------------\n";
    }
    exit($code);
}

sub setprefix {
    my $in = shift @_;
    if ($in && $in ne "") {
	$prefix = $in;
    }
    return(0);
}

sub setremote {
    my $remoteip = shift @_ || $current_artifacts{remoteip};
    
    if ($remoteip && $remoteip ne "") {
	if ($current_artifacts{account} && $current_artifacts{user}) {
	    $remote_pre = "ssh -o StrictHostKeyChecking=no root\@$remoteip 'source /root/$current_artifacts{account}-$current_artifacts{user}-qa/eucarc; export EC2_URL=http://$remoteip:8773/services/Eucalyptus; ";
	} else {
	    $remote_pre = "ssh -o StrictHostKeyChecking=no root\@$remoteip 'source /root/eucarc; export EC2_URL=http://$remoteip:8773/services/Eucalyptus; ";
	}
	$remote_post = "'";
	$cmd = "$runat $remote_pre uname -a $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($crc || $rc) {
	    doexit(1, "FAILED: could not run remote command: ($crc, $rc, $buf) = $cmd\n");
	}

	$cmd = "$runat $remote_pre ls -l /opt/eucalyptus/usr/sbin/euca_conf $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if (!$crc) {
	    $remote_pre .= "export PATH=/opt/eucalyptus/usr/sbin:\$PATH; export EUCALYPTUS=/opt/eucalyptus; ";
	    $current_artifacts{eucahome} = "/opt/eucalyptus";
	} else {
	    $remote_pre .= "export EUCALYPTUS=/; ";
	    $current_artifacts{eucahome} = "/";
	}

	if ($current_artifacts{"ec2apilocation"} ne "") {
	    $remote_pre .= "export EC2_HOME=$current_artifacts{ec2apilocation}; export PATH=$current_artifacts{ec2apilocation}/bin:\$PATH; ";
	}

	$cmd = "$runat $remote_pre readlink -f \`which java\` $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep -v RUNATCMD | grep java", "$ofile");
	if ($buf ne "") {
	    $buf =~ s/\/bin\/java//g;
	    $remote_pre .= "export JAVA_HOME=$buf; export PATH=$buf/bin:\$PATH; ";
	}

	$current_artifacts{remoteip} = "$remoteip";
    }
    return(0);
}

sub setlibsleep {
    my $insleep = shift @_;
    if ($insleep >= 0 && $insleep < 3600) {
	$library_sleep = $insleep;
    } else {
	$library_sleep = 0;
    }
}

sub build_and_deploy_fakeCC {
    my @ccips, $ccidx=0, $key;
    
    foreach $key (keys(%masters)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $masters{$key};
	    $ccidx++;
	}
    }
    foreach $key (keys(%slaves)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $slaves{$key};
	    $ccidx++;
	}
    }
    
    sleep($library_sleep);
    $done=0;
    my $j;
    for ($j=0; $j<$ccidx; $j++) {
	$ccip = $ccips[$j];
	if ($ccip =~ /\d+\.\d+\.\d+\.\d+/) {
	    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'cd /root/euca_builder/$current_artifacts{branch}/cluster/; make fake'";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if (!$rc && !$crc) {
		$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'cd /root/euca_builder/$current_artifacts{branch}/cluster/; make fakedeploy; cp $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf.orig; cat $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf | grep -v '^VNET_PUBLICIPS' > /tmp/meh.conf; " . 'echo VNET_PUBLICIPS=\"1.3.0.1-1.3.0.254 1.3.1.1-1.3.1.254\" >> /tmp/meh.conf' . "; cp /tmp/meh.conf $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf'";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if (!$rc && !$crc) {
		    $done++;
		    print "\tbuilt reconfigured and deployed fakeCC on $ccip\n";
		}
	    } else {
		print "\tfailed to build/reconfigure/deploy fakeCC on $ccip\n";
	    }
	}
	if (!$done) {
	    doexit(1, "FAILURE: could not install fakeCC on any CC\n");
	}
    }

    $oldrunat = "$runat";
    setrunat("runat 30");
    print "SUCCESS: set command timeout to 'runat 30'\n";

    control_component("STOP", "CC00", "MASTER");
    control_component("STOP", "CC00", "SLAVE");
    control_component("STOP", "CLC", "MASTER");
    control_component("STOP", "CLC", "SLAVE");
    control_component("START", "CLC", "MASTER");
    control_component("START", "CLC", "SLAVE");
    control_component("START", "CC00", "MASTER");
    control_component("START", "CC00", "SLAVE");

    setrunat("$oldrunat");
    print "SUCCESS: set command timeout to '$oldrunat'\n";
    return(0);
}



sub confirm_fakeCC {
    my $done=0, $i;
    for ($i=0; $i<30 && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
	($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$4}'", "ubero");
	$m1scount = int($m1scount);
	if (!$crc && !$rc && $m1scount > 2048) {
	    $done++;
	} else {
	    print "\twaiting for CLC to come back up with fakeCC\n";
	    sleep(1);
	}
    }
    if (!$done) {
	doexit(1, "FAILURE: CLC did not come back up with fakeCC\n");
    }
    discover_vmtypes();
    return(0);
}

sub restore_realCC {
    return(0);
}

sub run_fake_instance_scale {
    my $num = shift @_ || "10";
    my $emi = $current_artifacts{"$emitype"};

    my $currnum = 0;
    while($currnum < $num) {
	$newgroup = "iscale" .  int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10));
	$cmd = "$runat $remote_pre $prefix-add-group $newgroup -d '$newgroup' $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILURE: could not add new group $newgroup\n");
	}
	$cmd = "$runat $remote_pre $prefix-run-instances -n 28 $emi -g $newgroup $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILED: could not run more instances (curr=$currnum, goal=$num)\n");
	}
	$currnum += 28;
    }
    
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
    if ($crc || $rc || !$buf ) {
	print "\tfailed to describe instances after all runs\n";
    } else {
	my @insts = split(/\s+/, $buf);
	$cleanup_artifacts{instances} = join(" ", @insts);
    }

    return(0);
}

sub confirm_fake_instance_scale {
    my $num = shift @_ || "10";
    my $currnum = 0;

    my $done=0;
    my $i;
    for ($i=0; $i<160 && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep running | grep INST | awk '{print \$2}' | sort | uniq | wc | awk '{print \$1}'", "ubero");
	$currnum = int($buf);
	$current_artifacts{numinsts} = $currnum;
	if ($currnum >= $num) {
	    print "\tfound all instances running (curr=$currnum goal=$num)\n";
	    $done++;
	} else {
	    print "\twaiting for all instances to go to running (curr=$currnum goal=$num)\n";
	    sleep(1);
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for $num instances to go to running\n");
    }
    return(0);
}

sub do_instance_churn {
    my $num = shift @_ || "10";
    my $emi = $current_artifacts{"$emitype"};
    my $inst;
    my $currnum = 0, $realruns = 0;

    while($currnum < $num) {
	$newgroup = "iresponse" .  int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10));
	$cmd = "$runat $remote_pre $prefix-add-group $newgroup -d '$newgroup' $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILURE: could not add new group $newgroup\n");
	}
	$cmd = "$runat $remote_pre $prefix-run-instances -n 10 $emi -g $newgroup $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
	my @meh = split(/\s+/, $buf);
	my $numinsts = @meh;
	my $insts = join(" ", @meh);
	if ($crc || $rc || !$insts ) {
	    print "\tWARN: could not run more instances (curr=$currnum, goal=$num)\n";
	} else {
	    print "\tran instances $insts\n";
	    $realruns+=$numinsts;
	    
	    $cmd = "$runat $remote_pre $prefix-terminate-instances $insts $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	    if ($crc || $rc) {
		print "\tWARN: failed to terminate instance $inst\n";
	    }
	}
	$currnum+=10;
    }
    
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
    if ($crc || $rc || !$buf ) {
	print "\tfailed to describe instances after all runs\n";
    } else {
	my @insts = split(/\s+/, $buf);
	$cleanup_artifacts{instances} = join(" ", @insts);
    }

    print "\trealruns: $realruns goal: $num attempts: $currnum\n";
    
    return(0);    
}

sub test_response_time {
    my $cmd, $count, $sum;
    my @timings;

    $oldrunat = $runat;
    setrunat("runat 300");
    my $i;
    
    $sum = $count = 0;
    $cmd = "$runat $remote_pre (for i in `seq 1 101`; do /usr/bin/time -v sh -c $prefix-describe-instances; done) $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep Elapsed | awk '{print \$8}'", "ubero");
    my @times = split(/\s+/, $buf);
    my $j;
    for ($j=0; $j<@times; $j++) {
	if ($times[$j] =~ /(\d+):(\d+)\.(\d+)/) {
	    my $tot = 0;
	    $tot += $1*60;
	    $tot += $2;
	    $tot += $3 / 100;
	    $timings[$count] = $tot;
	    $sum += $tot;
	    $count++;
	}
    }
    if ($count > 0) {
	$mean = $sum / $count;
	$median = $timings[int($count/2)];
    } else {
	$mean = 0;
	$median = 0;
    }
    
    print "\ttrial_timings: @timings\n";
    print "\tstatistics: totaltrials=$count average=$mean median=$median\n";
    $current_artifacts{responseavg} = $mean;
    $current_artifacts{responsemed} = $median;
    setrunat("$oldrunat");
    return(0);
}

sub record_clc_state {
    sleep(300);
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "sort -n", "ubero");
    if ($crc || $rc) {
	doexit(1, "FAILED: could not describe instances\n");
    }
    $current_artifacts{clcstate} = $buf;

    $cmd = "$runat $remote_pre $prefix-describe-addresses $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "sort -n", "ubero");
    if ($crc || $rc) {
	doexit(1, "FAILED: could not describe addresses\n");
    }
    $current_artifacts{clcstate} .= $buf;

    return(0);
}

sub compare_clc_states {
    my $oldstate = shift @_;

    if ($current_artifacts{clcstate} ne "$oldstate") {
	print "\tWARN: oldstate and newstate differ:\n";
	print "\t--------OLDSTATE-------\n";
	print "$oldstate\n";
	print "\t--------NEWSTATE-------\n";
	print "$current_artifacts{clcstate}\n";
    }
    return(0);
}

sub setbfeimagelocation {
    my $url = shift @_ || $bfe_image;
    $bfe_image = $url;
}

sub populate_volume_with_image {
    # write bfebs image to attached instance volume
    $ip = $current_artifacts{"instanceip"};
    $idev = $current_artifacts{"instancedevice"};

    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }

    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) || ! ($idev =~ /\/dev\//)) {
	doexit(1, "populate_volume(): no IP ($ip) of local EBS device name ($idev)\n");
    }

    run_instance_command("echo 192.168.7.65 archive.ubuntu.com >> /etc/hosts");
    run_instance_command("echo 192.168.7.65 security.ubuntu.com >> /etc/hosts");
    $oldrunat = $runat;
    setrunat("runat 120");
    run_instance_command("apt-get update; true");
    run_instance_command("apt-get install -y curl; true");
    if ( $bfe_image =~ /windows.*/) {                        
        setrunat("runat 7200");
    } else {
        setrunat("runat 1200");
    }
    run_instance_command("curl $bfe_image > $idev");
    setrunat("$oldrunat");

    return(0);
}

sub stop_instance{
    my $inst = shift @_ || $current_artifacts{instance};
    if ( $bfe_image =~ /windows.*/){
         print "sleeping 180 seconds before stopping the instance\n"; 
         sleep(180); # let's wait until windows vm fully boots
    } 

    $cmd = "$runat $remote_pre $prefix-stop-instances $inst $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc) {
    	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub wait_for_stopped_instance {
    my $inst = shift @_ || $current_artifacts{instance};

    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep INSTANCE | awk '{print \$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state eq "stopped") {
	    $done++;
	} elsif ($state eq "shutting-down" || $state eq "terminated") {
	    doexit(1, "FAILED: waiting for instance to stop (state went to $state)\n");
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance to stop\n");
    }
    $current_artifacts{instancestate} = "stopped";

    return(0);
}


sub start_instance{
    my $inst = shift @_ || $current_artifacts{instance};

    $cmd = "$runat $remote_pre $prefix-start-instances $inst $remote_post";
    ($rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
    	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub reboot_instance{
    my $inst = shift @_ || $current_artifacts{instance};

    $cmd = "$runat $prefix-reboot-instances $inst";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
    	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub setrandomip{
    my $toip = shift @_;

    my $ip_part = int(rand(256)) + 1;

    my $ip = "192\.168\.38\.$ip_part";
    print "Setting IP: $ip on $toip\n";

    if (! ($toip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: bad input IP: toip=$toip\n");
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: bad input IP: ip=$ip\n");
    }
    
    print "\tAdding IP: $ip from $toip\n";
    my $cmd = "ssh -o StrictHostKeyChecking=no root\@$toip ip addr add $ip/18 dev eth0";
    run_command("$cmd");
    $current_artifacts{"arbitrator_ip"} = $ip;

    return(0);
}

sub removeip{
    my $toip = shift @_;
    my $ip = shift @_ || $current_artifacts{"arbitrator_ip"};

    if (! ($toip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: bad input IP: toip=$toip\n");
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: bad input IP: ip=$ip\n");
    }

    print "\tDeleting IP: $ip from $toip\n";
    my $cmd = "ssh -o StrictHostKeyChecking=no root\@$toip ip addr del $ip/18 dev eth0";
    run_command("$cmd");

    if ($ip eq $current_artifacts{"arbitrator_ip"}) {
	$current_artifacts{"arbitrator_ip"} = "";
    }
    return(0);
}

1;

