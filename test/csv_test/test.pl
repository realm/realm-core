@files = <*.csv>;
foreach $file (@files) {
	unlink("out.realm");

	print "\n\n\n\n***********************************************************\ntesting $file\n***********************************************************\n\n\n\n";


    system("../../csv.exe", $file, "out.realm");
	
	if ( $? != 0 )
	{
		print "\n\n\n\n***********************************************************\n$file failed!\n***********************************************************\n\n\n\n\n\n";
		die();
	}
	
	
}

print "\n\n\n\n***********************************************************\nAll databases were successfully converted!\n\nHOWEVER, please manually verify that float, int, etc, columns are recognized correctly and not just converted to String type\n***********************************************************\n\n\n\n\n\n";
