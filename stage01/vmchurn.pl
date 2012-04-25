#!/usr/bin/perl

require "ec2ops.pl";

my $account = shift @ARGV || "eucalyptus";
my $user = shift @ARGV || "admin";

# need to add randomness, for now, until account/user group/keypair
# conflicts are resolved
$rando = int(rand(10)) . int(rand(10)) . int(rand(10));
if ($account ne "eucalyptus") {
    $account .= "$rando";
}
if ($user ne "admin") {
    $user .= "$rando";
}
$newgroup = "vmchurngroup$rando";
$newkeyp = "vmchurnkey$rando";

parse_input();
print "SUCCESS: parsed input\n";

setlibsleep(1);
print "SUCCESS: set sleep time for each lib call\n";

setremote($masters{"CLC"});
print "SUCCESS: set remote CLC: masterclc=$masters{CLC}\n";

discover_emis();
print "SUCCESS: discovered loaded image: current=$current_artifacts{instancestoreemi}, all=$static_artifacts{instancestoreemis}\n";

discover_zones();
print "SUCCESS: discovered available zone: current=$current_artifacts{availabilityzone}, all=$static_artifacts{availabilityzones}\n";

discover_vmtypes();
print "SUCCESS: discovered vmtypes: m1smallmax=$static_artifacts{m1smallmax} m1smallavail=$static_artifacts{m1smallavail}\n";
#doexit(0, "YAY");
if ( ($account ne "eucalyptus") && ($user ne "admin") ) {
# create new account/user and get credentials
    create_account_and_user($account, $user);
    print "SUCCESS: account/user $current_artifacts{account}/$current_artifacts{user}\n";
    
    grant_allpolicy($account, $user);
    print "SUCCESS: granted $account/$user all policy permissions\n";
    
    get_credentials($account, $user);
    print "SUCCESS: downloaded and unpacked credentials\n";
    
    source_credentials($account, $user);
    print "SUCCESS: will now act as account/user $account/$user\n";
}

# moving along
add_keypair("$newkeyp");
print "SUCCESS: added new keypair: $current_artifacts{keypair}, $current_artifacts{keypairfile}\n";

add_group("$newgroup");
print "SUCCESS: added group: $current_artifacts{group}\n";

authorize_ssh();
print "SUCCESS: authorized ssh access to VM\n";

for ($i=0; $i<4; $i++) {
    
    discover_zones();
    print "SUCCESS: discovered zones: $static_artifacts{availabilityzones}\n";

    discover_vmtypes();
    print "SUCCESS: discovered vmtypes: m1smallmax=$static_artifacts{m1smallmax} m1smallavail=$static_artifacts{m1smallavail}\n";

    my @zones = split(/\s+/, $static_artifacts{availabilityzones});
    foreach $zone (@zones) {
	my $key = $zone . "m1smallavail";
#	my $key = $zone . "m1smallmax";				### FIX by KYO	022912	try to set the max instances limit to # of addr
	$current_artifacts{availabilityzone} = "$zone";
	if ($static_artifacts{$key}) {
	    run_instances($static_artifacts{$key});
	    print "SUCCESS: ran instances: $current_artifacts{instances}\n";
	    
	    wait_for_instance();
	    print "SUCCESS: instance went to running: $current_artifacts{instancestate}\n";
	    
	    terminate_instances($static_artifacts{$key});
	    print "SUCCESS: terminated " . $static_artifacts{$key} . " instances\n";
	} else {
	    sleep(15);
	}
    }
}

#wait_for_instance_ip();
#print "SUCCESS: instance got public IP: $current_artifacts{instanceip}\n";

doexit(0, "EXITING SUCCESS\n");

run_instance_command("umount /tmp/testmount");
print "SUCCESS: formatted, mounted, copied data to, and unmounted volume\n";

