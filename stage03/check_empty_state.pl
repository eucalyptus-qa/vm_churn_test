#!/usr/bin/perl

use List::Util qw[min max];

my %working_artifacts, %current_artifacts, %metadata;
my $runat = "runat 30";
my $prefix = "euca";
my $trycount = 300;
unlink("ubero");


my $inputfile = "../input/2b_tested.lst";
open(FH, "$inputfile") or die "failed to open $inputfile";
while(<FH>) {
    chomp;
    my $line = $_;
    my ($ip, $distro, $version, $arch, $source, @component_str) = split(/\s+/, $line);
    if ($ip =~ /\d+\.\d+\.\d+\.\d+/ && $distro && $version && ($arch eq "32" || $arch eq "64") && $source && @component_str) {
	foreach $component (@component_str) {
	    $component =~ s/\[//g;
	    $component =~ s/\]//g;
	    print "C: $component\n";
	    if ($masters{"$component"}) {
		$slaves{"$component"} = $masters{"$component"};
	    }
	    $masters{"$component"} = "$ip";
	    $roles{"$component"} = 1;
	    if ($distro =~ /VMWARE/ && $component =~ /NC(\d+)/) {
		$cc_has_broker{"CC$1"} = 1;
	    }
	}
    }
}
close(FH);
foreach $component (keys(%roles)) {
    print "Component: $component Master: $masters{$component} Slave: $slaves{$component}\n";
}

my $masterclc = $masters{"CLC"};
my $i;
#$runat = "echo runat 10 ssh root\@$masterclc";
$runat = "runat 10 ssh root\@$masterclc 'source /root/eucarc; ";

$rc = system("euca-terminate-instances `euca-describe-instances | grep INST | awk '{print \$2}'` 2>/dev/null >/dev/null");
sleep(5);
$rc = system("euca-terminate-instances `euca-describe-instances | grep INST | awk '{print \$2}'` 2>/dev/null >/dev/null");

$cmd = "$runat $prefix-describe-instances'";
($rc, $count) = piperun($cmd, "grep INST | wc | awk '{print \$1}'", "ubero");
if ($rc || $count > 0) {
    print "ERROR: describe-instances is not empty\n";
}

$cmd = "$runat $prefix-describe-availability-zones verbose'";
($rc, $m1savail) = piperun($cmd, "grep m1.small | awk '{print \$4}'", "ubero");
$m1savail = int($m1savail);
if ($rc || !$m1savail || $m1savail <= 0) {
    doexit(1, "FAILED: ($rc, $m1savail) = $cmd\n");
}

$cmd = "$runat $prefix-describe-availability-zones verbose'";
($rc, $m1smax) = piperun($cmd, "grep m1.small | awk '{print \$6}'", "ubero");
$m1smax = int($m1smax);
if ($rc || !$m1smax || $m1smax <= 0) {
    doexit(1, "FAILED: ($rc, $m1smax) = $cmd\n");
}

if ($m1savail != $m1smax) {
    print "ERROR: number of m1.small available != number of m1.small max ($m1savail != $m1smax)\n";
    exit(1);
}
exit(0);


sub piperun {
    my $cmd = shift @_;
    my $pipe = shift @_;
    my $uberofile = shift @_ || "/tmp/uberofile.$$";
    my $pipestr = "";

    if ($pipe) {
	$pipestr = "| $pipe";
    }
    
    system("$cmd > /tmp/tout.$$ 2>&1");
    chomp(my $buf = `cat /tmp/tout.$$ $pipestr`);
#    print "cat /tmp/tout.$$ $pipestr\n";
    system("echo CMD=$cmd >> $uberofile");
    my $rc = system("cat /tmp/tout.$$ >> $uberofile");

    unlink("/tmp/tout.$$");
    sleep(1);
    return($rc, $buf);
}

sub doexit {
    my $code = shift @_;
    my $msg = shift @_;

    foreach $inst (split(/\s+/, $working_artifacts{"insts"})) {
	$cmd = "$runat $prefix-terminate-instances $inst'";
	($rc, $buf) = piperun($cmd, "", "ubero");
    }

    foreach $emi (split(/\s+/, $working_artifacts{"emis"})) {
	$cmd = "$runat $prefix-deregister $emi'";
	($rc, $buf) = piperun($cmd, "", "ubero");
    }

    foreach $vol (split(/\s+/, $working_artifacts{"vols"})) {
	$cmd = "$runat $prefix-delete-volume $vol'";
	($rc, $buf) = piperun($cmd, "", "ubero");
    }

    foreach $snap (split(/\s+/, $working_artifacts{"snaps"})) {
	$cmd = "$runat $prefix-delete-snapshot $snap'";
	($rc, $buf) = piperun($cmd, "", "ubero");
    }

    if ($msg) {
	print STDERR "$msg";
    }
    exit($code);
}
