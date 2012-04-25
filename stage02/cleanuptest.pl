#!/usr/bin/perl

open (STDERR, ">&STDOUT");

$ec2timeout = 20;
$mode = shift @ARGV;

if( $mode eq "" ){
        my $this_mode = `cat ../input/2b_tested.lst | grep NETWORK`;
        chomp($this_mode);
        if( $this_mode =~ /^NETWORK\s+(\S+)/ ){
                $mode = lc($1);
        };
};

print "Mode:\t$mode \n\n";

if ($mode eq "system" || $mode eq "static") {
    $managed = 0;
} else {
    $managed = 1;
}

print "\n";
print "######################### Clean up Running Instances ###########################\n";
print "\n";


$done=0;
for ($i=0; $i<50 && !$done; $i++) {
	# clean up running instances
	print "\n";
	print "+++++ Trial $i +++++\n";
	print "\n";

	print "Discovering Running Instances\n";
	print "\n";
	
	print "Date: ";
	system("date");
	print "Command: runat $ec2timeout euca-describe-instances\n";
	print "\n";
	system("runat $ec2timeout euca-describe-instances");
	print "\n";

	sleep(1);

	print "\n";
	print "Capturing Instance IDs\n";
	print "\n";
	
	print "Date: ";
	system("date");
	print "Command: runat $ec2timeout euca-describe-instances | grep INST | awk '{print \$2}'\n";
	print "\n";
	chomp($instIds=`runat $ec2timeout euca-describe-instances | grep INST | awk '{print \$2}'`);
	$instIds=~s/\n/ /g;
	
	print "\n";
	print "Detected INSTIDs\n";
	print "INSTIDS: $instIds\n";
	print "\n";
	if ($instIds) {
		my @temp_array = split(" ", $instIds);
		foreach my $myid (@temp_array){
			if( request_terminate_instance("euca", $ec2timeout, $myid ) ){
				$done++;
			};
			sleep(1);
		};

		print "\n";
		print "Sleeping for 10 sec\n";
		print "\n";
		sleep(10);
	}else{
		print "There is no instances to terminate\n";
		$done++;
		print "\n";
	};
	print "\n";
};

# clean up keypairs
$count=0;
system("date");
$cmd = "runat $ec2timeout euca-describe-keypairs";
open(RFH, "$cmd|");
while(<RFH>) {
    chomp;
    my $line = $_;
    my ($tmp, $kp) = split(/\s+/, $line);
    if ($kp) {
	$kps[$count] = $kp;
	$count++;
    }
}
close(RFH);
if (@kps < 1) {
    print "WARN: could not get any keypairs from euca-describe-keypairs\n";
} else {
    for ($i=0; $i<@kps; $i++) {
	system("date");
$cmd = "runat $ec2timeout euca-delete-keypair $kps[$i]";
	$rc = system($cmd);
	if ($rc) {
	    print "ERROR: failed - '$cmd'\n";
	}
	system("rm $kps[$i].priv");
    }
}

# clean up groups
$count=0;
system("date");
$cmd = "runat $ec2timeout euca-describe-groups";
open(RFH, "$cmd|");
while(<RFH>) {
    chomp;
    my $line = $_;
    my ($type, $foo, $group) = split(/\s+/, $line);
    if ($type eq "GROUP") {
	if ($group && $group ne "default") {
	    $groups[$count] = $group;
	    $count++;
	}
    }
}
close(RFH);
if (@groups < 1) {
    print "WARN: could not get any groups from euca-describe-groups\n";
} else {
    for ($i=0; $i<@groups; $i++) {
	system("date");
$cmd = "runat $ec2timeout euca-revoke $groups[$i] -P icmp -s 0.0.0.0/0 -t -1:-1";
	$rc = system($cmd);
	if ($rc) {
	    print "ERROR: failed - '$cmd'\n";
	}
	system("date");
$cmd = "runat $ec2timeout euca-revoke $groups[$i] -P tcp -p 22 -s 0.0.0.0/0";
	$rc = system($cmd);
	if ($rc) {
	    print "ERROR: failed - '$cmd'\n";
	}
	system("date");
$cmd = "runat $ec2timeout euca-delete-group $groups[$i]";
	$rc = system($cmd);
	if ($rc) {
	    print "ERROR: failed - '$cmd'\n";
	}
    }
}

if ($managed) {
# clean up addrs
    $count=0;
    system("date");
$cmd = "runat $ec2timeout euca-describe-addresses | grep admin";
    open(RFH, "$cmd|");
    while(<RFH>) {
	chomp;
	my $line = $_;
	my ($tmp, $ip) = split(/\s+/, $line);
	if ($ip) {
	    $ips[$count] = $ip;
	    $count++;
	}
    }
    close(RFH);
    if (@ips < 1) {
	print "WARN: could not get any addrs from euca-describe-addresses\n";
    } else {
	for ($i=0; $i<@ips; $i++) {
	    system("date");
$cmd = "runat $ec2timeout euca-disassociate-address $ips[$i]";
	    $rc = system($cmd);
	    if ($rc) {
		print "ERROR: failed - '$cmd'\n";
	    }
	    $cmd = "euca-release-address $ips[$i]";
	    $rc = system($cmd);
	    if ($rc) {
		print "ERROR: failed - '$cmd'\n";
	    }
	}
    }
}

print "\n";


exit(0);


############################### SUBROUTINES ###################################

sub request_terminate_instance{
	my $tool = shift @_;
	my $timeout = shift @_;
	my $this_id = shift @_;

	print "\n";
	print "Requesting Terminate Instance $this_id\n";
	print "\n";

	print "Date: ";
	system("date");

	print "Command: runat $timeout " . $tool . "-terminate-instances $this_id\n";
	my $command = "runat $timeout " . $tool . "-terminate-instances $this_id";
	print "\n";
	my $this_rc = system($command);
	if ($this_rc) {
		print "\n";
		print "ERROR: failed in requesting terminate instance $this_id\n\n";
		return 1;
	};

	print "\n";

	return 0;
};

1;

