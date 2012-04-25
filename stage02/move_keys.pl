#!/usr/bin/perl

my $last_stage = `tail -n 1 ../status/test.stat`;

my $stage = 0;

if( $last_stage =~ /STAGE(\d+)/ ){
	$stage = $1;
};

if( $stage > 0 ){
	$stage++;
	my $next_stage = sprintf("%02d", $stage);
	if( -e "./mykey0.priv" ){
		print "Found mykey0.priv\n";
		print "Moving mykey0.priv to ../stage$next_stage ../fallback ../postrun\n\n";	
		system("cp ./mykey0.priv ../stage$next_stage/.");
		system("cp ./mykey0.priv ../fallback/.");
		system("cp ./mykey0.priv ../postrun/.");
	};

	if( -e "./mykey1.priv" ){
		print "Found mykey1.priv\n";
		print "Moving mykey1.priv to ../stage$next_stage ../fallback ../postrun\n\n";	
        	system("cp ./mykey1.priv ../stage$next_stage/.");
		system("cp ./mykey1.priv ../fallback/.");
		system("cp ./mykey1.priv ../postrun/.");
	};
};
exit(0);

