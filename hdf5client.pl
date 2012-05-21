#!/usr/local/bin/perl
use CBSU::hdf5;

my $hdf5 = CBSU::hdf5->new("cbsuss06", "12001"); #server name or ip address, port number

my $file, $project, $chr, $taxadim, $posdim;

@tbl = $hdf5->FLIST();
print "\nFLIST command\n---\n";
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	foreach $txt (@tbl)
	{
		print "$txt\n";
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;

$file = $tbl[0];

print "\nPLIST command\n---\n";
@tbl = $hdf5->PLIST();
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	foreach $txt (@tbl)
	{
		print "$txt\n";
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;

$project = $tbl[0];

print "\nFINFO $file command\n---\n";
@tbl = $hdf5->FINFO($file);
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	foreach $txt (@tbl)
	{
		print "$txt\n";
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;


print "\nPINFO $project command\n---\n";
($attr_ref, $taxa_ref, $chrs_ref) = $hdf5->PINFO($project);
my %attr = %$attr_ref;
my %taxa = %$taxa_ref;
my @chrs = @$chrs_ref;
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	print "Attributes:\n";
	for $txt (keys %attr)
	{
		print "\t$txt\t" . $attr{$txt} . "\n";
	}
	print "Taxa:\n";
	for $txt (keys %taxa)
	{
		print "\t$txt\t" . $taxa{$txt} . "\n";
	}
	$taxadim = 1*$taxa{'dim1'};
	for($i=0; $i<=$#chrs; $i++)
	{
		$ii = $i + 1;
		print "Chromosome $ii:\n";
		my $lhash_ref = $hdf5->decode_hash($chrs[$i]);
		my %lhash = %$lhash_ref;
		for $txt (sort keys %lhash)
		{
			print "\t$txt\t" . $lhash{$txt} . "\n";
		}
		if($i == 0)
		{
			#store data for QUERY function
			$chr = $lhash{'name'};
			$posdim = 1*$lhash{'positions_dim1'};
		}
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;


#choose range fo query
$taxacount = 10;
if($taxacount>$taxadim){$taxacount=$taxadim};
$poscount = 50;
if($poscount>$posdim){$poscount=$posdim};
$posq1 = int(rand($posdim - $poscount));
$posq2 = $posq1 + $poscount;
$taxaq1 = int(rand($taxadim - $taxacount));
$taxaq2 = $taxaq1 + $taxacount;
print "\nQUERY $project $chr $posq1 $posq2 $taxaq1 $taxaq2 command (defaults)\n---\n";
my @posqarr;
$posqarray[0] = $posq1;
$posqarray[1] = $posq2;
my @taxaqarr;
$taxaqarr[0] = $taxaq1;
$taxaqarr[1] = $taxaq2;
#query with default parameters and minimal input list
my ($enc, $data_ref) = $hdf5->QUERY(
	'user' => 'serveradmin', 
	'password' => 'cbsu4ever', 
	'project' => $project, 
	'chr' => $chr, 
	'positions' => \@posqarray, 
	'taxa' => \@taxaqarr);
my @data = @$data_ref;
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	for($i=0; $i<=$#data; $i++)
	{
		if($enc == 1)
		{
			print $data[$i] . "\n";
		}
		else
		{
			my @arr = split '', $data[$i]; #get an array of ascii values, only every $enc is printable as character
			for($j=0; $j<=$#arr; $j++)
			{
				if($j % $enc == 0)
				{
					if($j>0){print " ";}
					print $arr[$j];
				}
				else
				{
					print " " . ord($arr[$j]);
				}
			}	
			print "\n";
		}
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;


$taxacount = 10;
if($taxacount>$taxadim){$taxacount=$taxadim};
$poscount = 100;
if($poscount>$posdim){$poscount=$posdim};
$posq1 = int(rand($posdim - $poscount));
$posq2 = $posq1 + $poscount;
$taxaq1 = int(rand($taxadim - $taxacount));
$taxaq2 = $taxaq1 + $taxacount;
print "\nQUERY $project $chr $posq1 $posq2 $taxaq1 $taxaq2 command (all parameters)\n---\n";
my @posqarr;
$posqarray[0] = $posq1;
$posqarray[1] = $posq2;
my @taxaqarr;
$taxaqarr[0] = $taxaq1;
$taxaqarr[1] = $taxaq2;
my ($enc, $data_ref) = $hdf5->QUERY(
	'user' => 'serveradmin',  #user name (name of the administartive user is 'serveradmin')
	'password' => 'cbsu4ever',  #user password
	'debug' => 0,	  	# print server communication for debugging
	'project' => $project,  #project name
	'chr' => $chr, 		#chromosome name
	'dest' => 'std',	#data destination: 'std' - IO stream, 'file' - file on server
	'format' => 'let',	#server output data format: 'let' - letters (one byte char array), 'num' - numbers (one byte each)
				#this option is not used by Perl module since output is always 2D array of bytes
	'orientation' => 'auto',#queried data orientation array: 'auto' - server decides, 'pf' - positions fast, 'tf' - taxa fast	
	'prange' => 'range',	#positions query range: 'range' - between two positions, 'list' - list of positions, 'all' - all positions
	'ptype' => 'indexes',	#type of positions queried: 'indexes' - index of the array, 'markers' - marker names, 'positions' - positions
	'pstride' => 2,		#server will read every 'pstride' element in the range , default 1
	'positions' => \@posqarray, #array of positions values to query
	'trange' => 'range',	#positions query range: 'range' - between two taxa, 'list' - list of taxa, 'all' - all taxa
	'ttype' => 'indexes',	#type of taxa queried: 'indexes' - index of the array, 'taxa' - taxa names
	'tstride' => 1,		#server will read every 'pstride' element in the range, default 1
	'taxa' => \@taxaqarr,	#array of taxa values to query
	);
my @data = @$data_ref;
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	for($i=0; $i<=$#data; $i++)
	{
		if($enc == 1)
		{
			print $data[$i] . "\n";
		}
		else
		{
			my @arr = split '', $data[$i]; #get an array of ascii values, only every $enc is printable as character
			for($j=0; $j<=$#arr; $j++)
			{
				if($j % $enc == 0)
				{
					if($j>0){print " ";}
					print $arr[$j];
				}
				else
				{
					print " " . ord($arr[$j]);
				}
			}	
			print "\n";
		}
	}
}
print "---\n";


