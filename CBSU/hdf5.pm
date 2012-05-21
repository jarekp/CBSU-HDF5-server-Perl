package CBSU::hdf5;

use strict;
use vars qw(@ISA $VERSION @EXPORT_OK);

use Carp;
use IO::Socket::INET;

require Exporter;

@ISA = qw(Exporter);
@EXPORT_OK = qw(
	&FLIST,
	&PLIST,
	&PINFO,
	&FINFO
);
$VERSION = '0.02';

sub new
{
    my ($class, $host, $port) = @_;
    if($host eq "")
    {
	croak "HOST variable is empty!\n";
    }
    if($port eq "")
    {
	croak "PORT variable is empty!\n";
    }

    my $self = {};
    $self->{HOST} = $host; 
    $self->{PORT} = $port; 
    $self->{ERR} = ""; 
    bless $self, $class;
    return $self;
}

sub open
{
    my $self=shift;
    
    croak "object not initialized" if (!ref($self));

    my $socket = new IO::Socket::INET (
	PeerHost => $self->{HOST},
	PeerPort => $self->{PORT},
	Proto => 'tcp'
	) or croak "Cannot open connection!\n";
    return $socket;
}

sub extract
{
	my $self = shift;
	my $tblin = shift;

	my @tbl = @$tblin;
	my $r = 0;
	my $istart=0;;
	my $iend=-1;
	my $iresult=1;
	for(my $i=0; $i<=$#tbl; $i++)
	{
		#if($r == 1)
		#{
		#	$r = 2;
		#	next;
		#}
		my $first;
		($first) = split / /, $tbl[$i];
		if($first eq "START")
		{
			$r=1;
			$istart = $i + 1;
		}
		if($first eq "END")
		{
			$iend = $i - 1;
			last;
		}
	}	
	if($iend == -1)
	{
		$iend = $#tbl;
		$self->{ERR} = $tbl[$iend];
		$iresult = -2;
	}
	elsif($tbl[$iend+2] eq "Command execution successful")
	{
		$self->{ERR} = "";
		$iresult = 1;
	}
	elsif($tbl[$iend+2] eq "Incomplete command sequence")
	{
		$self->{ERR} = "Incomplete command sequence";
		$iresult = -1;
	}
	else
	{
		$self->{ERR} = $tbl[$iend];
		$iresult = 0;
	}
	return ($istart, $iend, $iresult);	
}

sub trim
{
	my $self = shift;
	my $datain = shift;
	my $i1 = shift;
	my $i2 = shift;

	my @data = @$datain;
	my $rtr = $#data - $i2;
	for(my $i=0; $i<$i1; $i++)
	{
		shift @data;
	}
	for(my $i=0; $i<$rtr; $i++)
	{
		pop @data;
	}
	return @data;
}

sub FLIST
{
	my $self = shift;

	$| = 1;
	my $socket = $self->open();
	print  $socket "FLIST\n\n";
	
	my @data;
	while(my $txt = <$socket>)
	{
		chomp $txt;
		push @data, $txt;	
	}
	$socket->close();	
	my $i1;
	my $i2;
	my $ir;
	($i1, $i2, $ir) = $self->extract(\@data);
	if($ir == 1){@data = $self->trim(\@data, $i1, $i2);}
	shift @data;
	for(my $i=0; $i<=$#data; $i++)
	{
		(my $txttmp, $data[$i]) = split /\t/, $data[$i];
	}
	return @data;
}

sub PLIST
{
	my $self = shift;

	$| = 1;
	my $socket = $self->open();
	print  $socket "PLIST\n\n";
	
	my @data;
	while(my $txt = <$socket>)
	{
		chomp $txt;
		push @data, $txt;	
	}
	$socket->close();	
	my $i1;
	my $i2;
	my $ir;
	($i1, $i2, $ir) = $self->extract(\@data);
	if($ir == 1){@data = $self->trim(\@data, $i1, $i2);}
	shift @data;
	for(my $i=0; $i<=$#data; $i++)
	{
		(my $txttmp, $data[$i]) = split /\t/, $data[$i];
	}
	return @data;
}


sub FINFO
{
	my $self = shift;
	my $file = shift;

	if($file eq "")
	{
		carp "file name empty";
		return;
	}
	$| = 1;
	my $socket = $self->open();
	print  $socket "FINFO\n$file\n\n";
	
	my @data;
	while(my $txt = <$socket>)
	{
		chomp $txt;
		push @data, $txt;	
	}
	$socket->close();	
	my $i1;
	my $i2;
	my $ir;
	($i1, $i2, $ir) = $self->extract(\@data);
	if($ir == 1){@data = $self->trim(\@data, $i1, $i2);}
	shift @data;
	for(my $i=0; $i<=$#data; $i++)
	{
		(my $txttmp, $data[$i]) = split /\t/, $data[$i];
	}
	return @data;
}

sub _encode_hash
{
	my $hashin_ref = shift;
	my %hashin =  %$hashin_ref;

	my $data = "";

	for my $txt (keys %hashin)
	{
		if($data ne ""){$data .= "\n";}
		$data .= $txt . "\t" .$hashin{$txt};
	}
	return $data;
}

sub decode_hash
{
	my $self = shift;
	my $datain = shift;
	
	my %hashout;

	my @tmptbl1 = split /\n/, $datain;
	for(my $i=0; $i<=$#tmptbl1; $i++)
	{
		my @tmptbl2 = split /\t/, $tmptbl1[$i];
		$hashout{$tmptbl2[0]} = $tmptbl2[1];
	}
	return \%hashout;
}

sub PINFO
{
#PINFO returns the following list (array_of_attributes, taxa_info_hash, array_of_chromosomes).
#attributes_hash
#taxa_info_hash{dim1, indexed}
#array_of_chromosomes is array of packed hashes: {name, data_pf_dim1, data_pf_dim2, data_tf_dim1, data_tf_dim2, positions_dim1, markers_dim1, markers_indexed}
	my $self = shift;
	my $project = shift;
	
	my %attributes_hash;
	my @array_of_chromosomes;
	my %taxa_info_hash = ('dim1' => 0, 'indexed' => 0);

	if($project eq "")
	{
		carp "project name empty";
		return;
	}
	$| = 1;
	my $socket = $self->open();
	print  $socket "PINFO\n$project\n\n";
	
	my @data;
	while(my $txt = <$socket>)
	{
		chomp $txt;
		push @data, $txt;	
	}
	$socket->close();	
	my $i1;
	my $i2;
	my $ir;
	($i1, $i2, $ir) = $self->extract(\@data);
	if($ir == 1)
	{
		@data = $self->trim(\@data, $i1, $i2);
	}
	else
	{
		return;
	}
	my $attr = 1;
	my $chr = 0;
	my %chr_hash = {};
	shift @data;
	for(my $i=0; $i<=$#data; $i++)
	{
		my $first;
		my $s1;
		my $s2;
		my $s3;
		($first, $s1, $s2, $s3) = split /\t/, $data[$i];
		if($first eq "=" && $s1 eq "taxa")
		{
			$attr = 0;
			$taxa_info_hash{'dim1'} = $s2;
			if($s3 eq "indexed"){$taxa_info_hash{'indexed'} = 1;}
			next;
		}
		elsif($first eq "chromosome")
		{
			if($chr>0)
			{
				my $chr_txt = _encode_hash(\%chr_hash);
				push @array_of_chromosomes, $chr_txt;
			}	
			%chr_hash = ('name' => $s2, 'data_pf_dim1' => 0, 'data_pf_dim2' => 0, 'data_tf_dim1' => 0, 'data_tf_dim2' => 0, 'positions_dim1' => 0, 'markers_dim1' => 0, 'markers_indexed' => "");
			$chr++;
			next;
		}
		if($attr == 1)
		{
			$attributes_hash{$first} = $s1;
			next;
		}
		if($s1 eq "data_array_pf")
		{
			$chr_hash{'data_pf_dim1'} = $s2;
			$chr_hash{'data_pf_dim2'} = $s3;
		}
		if($s1 eq "data_array_tf")
		{
			$chr_hash{'data_tf_dim1'} = $s2;
			$chr_hash{'data_tf_dim2'} = $s3;
		}
		if($s1 eq "positions")
		{
			$chr_hash{'positions_dim1'} = $s2;
		}
		if($s1 eq "markers")
		{
			$chr_hash{'markers_dim1'} = $s2;
			if($s3 eq "indexed"){$chr_hash{'markers_indexed'} = 1;}
		}
		
	}
	my @retarray = (\%attributes_hash, \%taxa_info_hash, \@array_of_chromosomes);
	return @retarray;
}

sub QUERY
{
	(my $self, my @arg) = @_;

	my %args = @arg;

#set default values for input args
	if($args{'project'} eq "")
	{
		$self->{ERR} = "Project name missing\n";
		return;
	}
	if($args{'chr'} eq "")
	{
		$self->{ERR} = "Chromosome name missing\n";
		return;
	}
	if($args{'user'} eq "")
	{
		$self->{ERR} = "User name missing\n";
		return;
	}
	if($args{'password'} eq "")
	{
		$self->{ERR} = "User password missing\n";
		return;
	}
	if($args{'debug'} eq ""){$args{'debug'} = '0';}
	if($args{'dest'} eq ""){$args{'dest'} = 'std';}
	if($args{'format'} eq ""){$args{'format'} = 'let';}
	if($args{'orientation'} eq ""){$args{'orientation'} = 'auto';}
	if($args{'prange'} eq ""){$args{'prange'} = 'range';}
	if($args{'ptype'} eq ""){$args{'ptype'} = 'indexes';}
	if($args{'trange'} eq ""){$args{'trange'} = 'range';}
	if($args{'ttype'} eq ""){$args{'ttype'} = 'indexes';}
	if($args{'pstride'} eq ""){$args{'pstride'} = '1';}
	if($args{'tstride'} eq ""){$args{'tstride'} = '1';}
	my $positions_ref = $args{'positions'};
	if($positions_ref == undef)
	{
		$self->{ERR} = "Missing positions array\n";
		return;
	}
	my @positions = @$positions_ref;
	if($#positions != 1 && $args{'prange'} eq 'range')
	{
		$self->{ERR} = "Positions type 'range' requires 2 positions in positions array\n";
		return;
	}
	my $taxa_ref = $args{'taxa'};
	if($taxa_ref == undef)
	{
		$self->{ERR} = "Missing taxa array\n";
		return;
	}
	my @taxa = @$taxa_ref;
	if($#taxa != 1 && $args{'trange'} eq 'range')
	{
		$self->{ERR} = "Taxa type 'range' requires 2 taxa in taxa array\n";
		return;
	}

	my $querystr = "QUERY\n";
	$querystr .= $args{'user'} . "\n";
	$querystr .= $args{'password'} . "\n";
	$querystr .= $args{'ptype'} . "\n";
	$querystr .= $args{'prange'} . "\n";
	$querystr .= ($#positions+1) . "\n";
	$querystr .= $args{'pstride'} . "\n";
	$querystr .= $args{'ttype'} . "\n";
	$querystr .= $args{'trange'} . "\n";
	$querystr .= ($#taxa+1) . "\n";
	$querystr .= $args{'tstride'} . "\n";
	$querystr .= $args{'dest'} . "\n";
	$querystr .= $args{'format'} . "\n";
	$querystr .= $args{'orientation'} . "\n";
	$querystr .= $args{'project'} . "\n";
	$querystr .= $args{'chr'} . "\n";
	for(my $i=0; $i<=$#positions; $i++)
	{
		$querystr .= $positions[$i] . "\n";
	}
	for(my $i=0; $i<=$#taxa; $i++)
	{
		$querystr .= $taxa[$i] . "\n";
	}
	$querystr .= "\n";
	
	$| = 1;
	my $socket = $self->open();
	if($args{'debug'} == '1')
	{
		print  $querystr;
	}
	print  $socket $querystr;
	
	my @data;
	while(my $txt = <$socket>)
	{
		if($args{'debug'} == '1')
		{
			print $txt;
		}
		chomp $txt;
		push @data, $txt;	
	}
	$socket->close();	

	my ($i1, $i2, $ir) = $self->extract(\@data);
	if($ir == 1)
	{
		@data = $self->trim(\@data, $i1, $i2);
	}
	else
	{
		return;
	}
	#convert data based on encoding
	shift @data;
	my @tmparr = split / +/, $data[0];
	shift @data;
	my $enc = 1*$tmparr[3];	
	if($enc == 1 && $args{'format'}  eq 'num')
	{
	}
	elsif($enc > 1 && $args{'format'}  eq 'let')
	{
	}
	elsif($enc > 1 && $args{'format'}  eq 'num')
	{
	}

	return ($enc, \@data);
}


1;
__END__

=head1 NAME

CBSU::hdf5 - A module to access CBSU hdf5 server

=head1 SYNOPSIS

use CBSU::hdf5;

my $hdf5 = CBSU::hdf5->new("yourhdf5servername", "12001"); #server name or ip address, port number

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
        'user' => 'username',
        'password' => 'userpassword',
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
        'user' => 'username',   #user name (name of the administartive user is 'serveradmin')
        'password' => 'pass',   #user password
        'debug' => 0,           # print server communication for debugging
        'project' => $project,  #project name
        'chr' => $chr,          #chromosome name
        'dest' => 'std',        #data destination: 'std' - IO stream, 'file' - file on server
        'format' => 'let',      #server output data format: 'let' - letters (one byte char array), 'num' - numbers (one byte each)
                                #this option is not used by Perl module since output is always 2D array of bytes
        'orientation' => 'auto',#queried data orientation array: 'auto' - server decides, 'pf' - positions fast, 'tf' - taxa fast       
        'prange' => 'range',    #positions query range: 'range' - between two positions, 'list' - list of positions, 'all' - all positions
        'ptype' => 'indexes',   #type of positions queried: 'indexes' - index of the array, 'markers' - marker names, 'positions' - positions
        'pstride' => 2,         #server will read every 'pstride' element in the range , default 1
        'positions' => \@posqarray, #array of positions values to query
        'trange' => 'range',    #positions query range: 'range' - between two taxa, 'list' - list of taxa, 'all' - all taxa
        'ttype' => 'indexes',   #type of taxa queried: 'indexes' - index of the array, 'taxa' - taxa names
        'tstride' => 1,         #server will read every 'pstride' element in the range, default 1
        'taxa' => \@taxaqarr,   #array of taxa values to query
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



=head1 DESCRIPTION

This module is a proxy for using CBSU hdf5 server. All non-administrative functions are implemented. See SYNOPSIS for more information on syntax.

=head1 AUTHOR

Jaroslaw Pillardy, jp86@cornell.edu

=head1 SEE ALSO

perl(1).

=cut

