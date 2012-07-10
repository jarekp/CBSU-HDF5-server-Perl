#!/usr/local/bin/perl
use CBSU::hdf5;

my $hdf5 = CBSU::hdf5->new("servername.domain.com", "12001"); #server name or ip address, port number

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


print "\nQUERY (defaults, output directly to client)\n---\n";
#choose range fo query
$taxacount = 10;
if($taxacount>$taxadim){$taxacount=$taxadim};
$poscount = 50;
if($poscount>$posdim){$poscount=$posdim};
$posq1 = int(rand($posdim - $poscount));
$posq2 = $posq1 + $poscount;
$taxaq1 = int(rand($taxadim - $taxacount));
$taxaq2 = $taxaq1 + $taxacount;
print "\nINPUT: project=$project chr=$chr positions: $posq1 $posq2 taxa: $taxaq1 $taxaq2 \n---\n";
my @posqarr;
$posqarray[0] = $posq1;
$posqarray[1] = $posq2;
my @taxaqarr;
$taxaqarr[0] = $taxaq1;
$taxaqarr[1] = $taxaq2;
#query with default parameters and minimal input list
my ($enc, $orientation, $np, $nt, $data_ref) = $hdf5->QUERY(
	'user' => 'serveradmin', 
	'password' => 'password_text', 
	'project' => $project, 
	'chr' => $chr, 
	'positions' => \@posqarray, 
	'taxa' => \@taxaqarr);
my @data = @$data_ref;
#data array contains actual data array with fast dimension in each line
# followed by positions vector, alleles vector, taxa vector and (optional) markers vector
my $nn;
print "*** $enc bytes per data point, orientation=$orientation positions=$np taxa=$nt\n";
if($orientation eq "pf")
{
	$nn = $nt;
}
else
{
	$nn = $np;
}
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	print "DATA ARRAY\n";
	for($i=0; $i<$nn; $i++)
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
	my $n1 = $nn;
	print "POSITIONS VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np;
	print "ALLELES VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np;
	print "TAXA VECTOR\n";
	for($i=$n1; $i<$n1+$nt; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np + $nt;
	print "MARKERS VECTOR\n";
	if($n1 < $#data)
	{
		for($i=$n1; $i<$n1+$np; $i++)
		{
			print $data[$i] . "\n";
		}
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;


print "\nQUERY (all parameters, output directly to client)\n---\n";
$taxacount = 10;
if($taxacount>$taxadim){$taxacount=$taxadim};
$poscount = 100;
if($poscount>$posdim){$poscount=$posdim};
$posq1 = int(rand($posdim - $poscount));
$posq2 = $posq1 + $poscount;
$taxaq1 = int(rand($taxadim - $taxacount));
$taxaq2 = $taxaq1 + $taxacount;
print "\nINPUT: project=$project chr=$chr positions: $posq1 $posq2 taxa: $taxaq1 $taxaq2 \n---\n";
my @posqarr;
$posqarray[0] = $posq1;
$posqarray[1] = $posq2;
my @taxaqarr;
$taxaqarr[0] = $taxaq1;
$taxaqarr[1] = $taxaq2;
my ($enc, $orientation, $np, $nt, $data_ref) = $hdf5->QUERY(
	'user' => 'serveradmin',  #user name (name of the administartive user is 'serveradmin')
	'password' => 'password_text',  #user password
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
#data array contains actual data array with fast dimension in each line
# followed by positions vector, alleles vector, taxa vector and (optional) markers vector
my $nn;
print "*** $enc bytes per data point, orientation=$orientation positions=$np taxa=$nt\n";
if($orientation eq "pf")
{
	$nn = $nt;
}
else
{
	$nn = $np;
}
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
else
{
	for($i=0; $i<$nn; $i++)
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
	my $n1 = $nn;
	print "POSITIONS VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np;
	print "ALLELES VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np;
	print "TAXA VECTOR\n";
	for($i=$n1; $i<$n1+$nt; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np + $nt;
	print "MARKERS VECTOR\n";
	if($n1 < $#data)
	{
		for($i=$n1; $i<$n1+$np; $i++)
		{
			print $data[$i] . "\n";
		}
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;

print "\nQUERY (defaults, output to a file on server)\n---\n";
#here we assume that serevr has been configured to make the results files accessible via URL
#please contact you server administrators for information on how they expose these files
#choose range fo query
$taxacount = 10;
if($taxacount>$taxadim){$taxacount=$taxadim};
$poscount = 50;
if($poscount>$posdim){$poscount=$posdim};
$posq1 = int(rand($posdim - $poscount));
$posq2 = $posq1 + $poscount;
$taxaq1 = int(rand($taxadim - $taxacount));
$taxaq2 = $taxaq1 + $taxacount;
print "\nINPUT: project=$project chr=$chr positions: $posq1 $posq2 taxa: $taxaq1 $taxaq2 \n---\n";
my @posqarr;
$posqarray[0] = $posq1;
$posqarray[1] = $posq2;
my @taxaqarr;
$taxaqarr[0] = $taxaq1;
$taxaqarr[1] = $taxaq2;
#query with default parameters and minimal input list
my ($filename) = $hdf5->QUERY(
	'user' => 'serveradmin', 
	'password' => 'password_text', 
	'dest' => 'file',	#data destination: 'std' - IO stream, 'file' - file on server
	'project' => $project, 
	'chr' => $chr, 
	'positions' => \@posqarray, 
	'taxa' => \@taxaqarr);
print "Server output file name $filename\n";
print "URL to read http://cbsuss06.tc.cornell.edu/hdfdata/$filename\n";
#reading the file 
my @data;
$urlerr = "";
if($hdf5->{ERR} eq "")
{
	use LWP;
	$ua=LWP::UserAgent->new;
	$ua->agent("MyApp/0.1 ");
	$adr = "http://cbsuss06.tc.cornell.edu/hdf5data/$filename";
	$req = HTTP::Request->new(GET => $adr);
	my $res = $ua->request($req);
	if ($res->is_success) 
	{
		@data = split /\n/, $res->content;
		shift @data;
	        my @tmparr = split / +/, $data[0];
       		$enc = 1*$tmparr[3];
		shift @data;
		@tmparr = split / +/, $data[0];
		$orientation = $tmparr[2];
		shift @data;
		@tmparr = split / +/, $data[0];
		$np = 1*$tmparr[3];
		shift @data;
		@tmparr = split / +/, $data[0];
		$nt = 1*$tmparr[3];
		shift @data;
	}
	else
	{
		$urlerr = "ERROR: Can't open URL\n";
	}
}

#data array contains actual data array with fast dimension in each line
# followed by positions vector, alleles vector, taxa vector and (optional) markers vector
my $nn;
print "*** $enc bytes per data point, orientation=$orientation positions=$np taxa=$nt\n";
if($orientation eq "pf")
{
	$nn = $nt;
}
else
{
	$nn = $np;
}
if($hdf5->{ERR} ne "")
{
	print "ERROR: ". $hdf5->{ERR} . "\n";
}
elsif($urlerror ne "")
{
	print $urlerror;
}
else
{
	print "DATA ARRAY\n";
	for($i=0; $i<$nn; $i++)
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
	my $n1 = $nn;
	print "POSITIONS VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np;
	print "ALLELES VECTOR\n";
	for($i=$n1; $i<$n1+$np; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np;
	print "TAXA VECTOR\n";
	for($i=$n1; $i<$n1+$nt; $i++)
	{
		print $data[$i] . "\n";
	}
	$n1 = $nn + $np + $np + $nt;
	print "MARKERS VECTOR\n";
	if($n1 < $#data)
	{
		for($i=$n1; $i<$n1+$np; $i++)
		{
			print $data[$i] . "\n";
		}
	}
}
print "---\n";
print "press ENTER to continue\n";
<>;
$table = 'alleles';
$starting_index=1;
$ending_index=23;
print "\nTABLE $project $chr $table $starting_index $ending_index command (all parameters)\n---\n";
my ($data_ref) = $hdf5->TABLE(
	'user' => 'serveradmin',  #user name (name of the administartive user is 'serveradmin')
	'password' => 'password_text',  #user password
	'project' => $project,  #project name
	'chr' => $chr, 		#chromosome name
	'table' => $table,	#name of the table: 'taxa', 'positions' or 'markers'
	'starting_index' => $starting_index,	#starting index of the table
	'ending_index' => $ending_index	#ending index of the table
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
		print $data[$i] . "\n";
	}
}
print "---\n";


