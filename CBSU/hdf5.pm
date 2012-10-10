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
	&TABLE,
	&QUERY,
	&USERPASS,
	&ADMINCMD,
	&LOGIN,
	&HAPMAP,
	&FINFO
);
$VERSION = '0.03';

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

	$self->{ERR} = "";
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
	(my $self, my @arg) = @_;
	my %args = @arg;
	$self->{ERR} = "";
	my $long = 0;
	if($args{'long'} eq "1"){$long=1;}

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
		(my $txttmp, my $txttmp1, my $txttmp2) = split /\t/, $data[$i];
		if($long == 0)
		{
			$data[$i] = $txttmp1;
		}
		else
		{
			$data[$i] = $txttmp1 . "\t" . $txttmp2;
		}	
	}
	return @data;
}


sub FINFO
{
	my $self = shift;
	my $file = shift;

	$self->{ERR} = "";
	if($file eq "")
	{
		$self->{ERR} = "file name empty";
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
	#shift @data;
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
	
	$self->{ERR} = "";
	my %attributes_hash;
	my @array_of_chromosomes;
	my %taxa_info_hash = ('dim1' => 0, 'indexed' => 0);

	if($project eq "")
	{
		$self->{ERR} = "project name empty";
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
	#shift @data;
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
			my $clen = substr($s3, 4);
			%chr_hash = ('name' => $s2, 'data_pf_dim1' => 0, 'data_pf_dim2' => 0, 'data_tf_dim1' => 0, 'data_tf_dim2' => 0, 'positions_dim1' => 0, 'markers_dim1' => 0, 'markers_indexed' => "", 'length' => $clen);
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
		if($s1 eq "alleles")
		{
			$chr_hash{'positions_dim1'} = $s2;
		}
		if($s1 eq "markers")
		{
			$chr_hash{'markers_dim1'} = $s2;
			if($s3 eq "indexed"){$chr_hash{'markers_indexed'} = 1;}
		}
		
	}
	if($chr>0)
	{
		my $chr_txt = _encode_hash(\%chr_hash);
		push @array_of_chromosomes, $chr_txt;
	}
	my @retarray = (\%attributes_hash, \%taxa_info_hash, \@array_of_chromosomes);
	return @retarray;
}

sub QUERY
{
	(my $self, my @arg) = @_;

	my %args = @arg;
	$self->{ERR} = "";

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
	my @positions = ();
	if($positions_ref == undef && $args{'prange'} ne "all")
	{
		$self->{ERR} = "Missing positions array\n";
		return;
	}
	elsif($positions_ref != undef && $args{'prange'} ne "all")
	{
		@positions = @$positions_ref;
	}
	if($#positions != 1 && $args{'prange'} eq 'range')
	{
		$self->{ERR} = "Positions type 'range' requires 2 positions in positions array\n";
		return;
	}
	my $taxa_ref = $args{'taxa'};
	my @taxa = ();
	if($taxa_ref == undef && $args{'trange'} ne "all")
	{
		$self->{ERR} = "Missing taxa array\n";
		return;
	}
	elsif($taxa_ref != undef && $args{'trange'} ne "all")
	{
		@taxa = @$taxa_ref;
	}
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
	#if destination is file, return file name
	if($args{'dest'} eq "file")
	{
		return ($data[0]);	
	}
	
	#convert data based on encoding
	shift @data;
	my @tmparr = split / +/, $data[0];
	my $enc = 1*$tmparr[3];	
	shift @data;
	@tmparr = split / +/, $data[0];
	my $orientation = $tmparr[2];
	shift @data;
	@tmparr = split / +/, $data[0];
	my $np = 1*$tmparr[3];
	shift @data;
	@tmparr = split / +/, $data[0];
	my $nt = 1*$tmparr[3];
	shift @data;
	if($enc == 1 && $args{'format'}  eq 'num')
	{
	}
	elsif($enc > 1 && $args{'format'}  eq 'let')
	{
	}
	elsif($enc > 1 && $args{'format'}  eq 'num')
	{
	}

	return ($enc, $orientation, $np, $nt, \@data);
}

sub TABLE
{
	(my $self, my @arg) = @_;

	my %args = @arg;
	$self->{ERR} = "";

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
	if($args{'starting_index'} eq "")
	{
		$self->{ERR} = "Starting index missing\n";
		return;
	}
	if($args{'ending_index'} eq "")
	{
		$self->{ERR} = "Ending index missing\n";
		return;
	}
	if($args{'table'} eq "")
	{
		$self->{ERR} = "Table name missing\n";
		return;
	}
	if($args{'table'} ne "positions" && $args{'table'} ne "taxa" && $args{'table'} ne "markers" && $args{'table'} ne "alleles")
	{
		$self->{ERR} = "Table name can only be 'positions', 'markers', 'alleles' or 'taxa'\n";
		return;
	}
	if($args{'position_type'} eq ""){$args{'position_type'} = 'index';}

	my $querystr = "TABLE\n";
	$querystr .= $args{'user'} . "\n";
	$querystr .= $args{'password'} . "\n";
	$querystr .= $args{'table'} . "\n";
	$querystr .= $args{'project'} . "\n";
	$querystr .= $args{'chr'} . "\n";
	$querystr .= $args{'position_type'} . "\n";
	$querystr .= $args{'starting_index'} . "\n";
	$querystr .= $args{'ending_index'} . "\n";
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

	return (\@data);
}

sub HAPMAP
{
	(my $self, my @arg) = @_;

	my %args = @arg;
	$self->{ERR} = "";

	my $data_ref = $args{'data'};
	my @data = ();
	if($data_ref == undef)
	{
		$self->{ERR} = "Missing data array\n";
		return;
	}
	if($args{'orientation'} eq undef)
	{
		$self->{ERR} = "Missing orientation parameter\n";
		return;
	}
	if($args{'filename'} eq undef)
	{
		$self->{ERR} = "Missing file name parameter\n";
		return;
	}
	if($args{'np'} eq undef)
	{
		$self->{ERR} = "Missing np parameter\n";
		return;
	}
	if($args{'nt'} eq undef)
	{
		$self->{ERR} = "Missing nt parameter\n";
		return;
	}
	if($args{'enc'} eq undef)
	{
		$self->{ERR} = "Missing enc parameter\n";
		return;
	}
	if($args{'chr'} eq undef)
	{
		$self->{ERR} = "Missing chr name parameter\n";
		return;
	}
	if($args{'genome_ver'} eq undef)
	{
		$self->{ERR} = "Missing genome_ver parameter\n";
		return;
	}
	my $progress;
	if($args{'progress'} eq undef)
	{
		$progress = 0;
	}
	else
	{
		$progress = $args{'progress'}
	}
	my $outformat = "IUPAC";
	if($args{"format"} ne undef && $args{"format"} ne ""){$outformat=$args{"format"};}
	my $tmpdir = "";
	my $outfname=$args{'filename'};
	my $orientation=$args{'orientation'};
	my $np=$args{'np'};
	my $nt=$args{'nt'};
	my $chr=$args{'chr'};
	my $genome_ver=$args{'genome_ver'};
	my $enc=$args{'enc'};
	my $flush=$args{'flush'};
	@data = @$data_ref;
	
	if($orientation eq "tf")
	{
		my $isfilenew = 1;
		if( -s $outfname) { $isfilenew = 0; }

		open(OUT,">>$outfname");

                my $nn = $np;
                my $n_taxa = $nn + $np + $np;
                my $n_marker = $nn + $np + $np + $nt;
                my $n_allele = $nn + $np;

		if($isfilenew)
		{
        		# print the beginning headers and the TAXA line (only if this is a new file being open)
			print OUT "rs#\talleles\tchrom\tpos\tstrand\tassembly#\tcenter\tprotLSID\tassayLSID\tpanelLSID\tQCcode\t";
        		for(my $ii=$n_taxa; $ii<$n_taxa+$nt; $ii++)
        		{
                		print OUT $data[$ii] . "\t";
        		}
			print OUT "\n";
		}  # if $isfilenew

		# Loop over positions
		my $allit = 0;
		for(my $pos=0;$pos<$np;$pos++)
		{
			$allit += length($data[$pos]); #get an array of ascii values, only every $enc is printable as character
		}
		my $curit = 0;
		my $pstep = 0;
		if($progress>0){$pstep = int($allit/$progress);}
		for(my $pos=0;$pos<$np;$pos++)
		{
			my $markerstr = $data[$pos+$n_marker];
			if($data[$pos+$n_marker] eq "Cannot open table markers" || $data[$pos+$n_marker] eq "")
			{
				$markerstr = $chr . "_" . $data[$pos+$np];	
			}
			# Print the marker
			print OUT "$markerstr";
			# Print the alleles and extract the individual ones (for numerical encoding)
			print OUT "\t$data[$pos+$n_allele]";
			my @alleles = split "/", $data[$pos+$n_allele];
			# print the chromosome
			print OUT "\t$chr";
			# print position and strand (always "+")
			print OUT "\t$data[$pos+$np]\t+";
			# print genome version (assembly#)
			print OUT "\t$genome_ver";
			# additional 5 columns - ask what they mean and if we can supply them...
			print OUT "\tNA\tNA\tNA\tNA\tNA";
			# print a row from the data matrix - somewhat tricky
			my @arr = split '', $data[$pos]; #get an array of ascii values, only every $enc is printable as character
			for(my $j=0; $j<=$#arr; $j++)       # this loop should be over taxa
                        {
				my $outchar = $arr[$j];
				if($outformat eq "NUMERIC") { $outchar = allele_encode($arr[$j], @alleles) };
                                if($j % $enc == 0)
                                {
                                        print OUT "\t$outchar";
                                }
                                else
                                {
                                        print OUT "\t" . ord($outchar);
                                }
				if($progress>0)
				{
					if($curit%$pstep==0)
					{
						my $ppp = int(1000*$curit/$allit)/10;
						$flush->("$curit/$allit $ppp\%");
					}
				}
				$curit++;
                        }
                        print OUT "\n";    # end of the row
		}
		if($progress>0){$flush->("$curit/$allit 100%");}

		close OUT;
	}      # end of orientation "tf" option
	else   # if orientation is "pf", a transposed HapMap file will be produced...
	{
		my $isfilenew = 1;
                if( -s $outfname) { $isfilenew = 0; }

		open(IN,$outfname);
                open(OUT,">${outfname}_append");

		my $nn = $nt;
                my $n_taxa = $nn + $np + $np;
                my $n_marker = $nn + $np + $np + $nt;
                my $n_allele = $nn + $np;		

		# Print line with markers
		my $line_old = "";
		if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
		if($isfilenew) { print OUT "rs#"; }
		my $pos;
		for($pos=0;$pos<$np;$pos++)
		{
			if($data[$pos+$n_marker] eq "" || $data[$pos+$n_marker] eq "Cannot open table markers")
			{
				print OUT "\t$chr" . "_$data[$pos+$nt]";
			}
			else
			{
				print OUT "\t$data[$pos+$n_marker]";
			}
		}
		print OUT "\n";

		# print line with alleles
		$line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
		if($isfilenew) { print OUT "alleles"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\t$data[$pos+$n_allele]";
                }
		print OUT "\n";

		# print line with chromosomes
		$line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
		if($isfilenew) { print OUT "chrom"; }
                for($pos=0;$pos<$np;$pos++)
                {
                         print OUT"\t$chr";
                }
		print OUT "\n";

		# print line with positions
		$line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
	 	if($isfilenew) { print OUT "pos"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\t$data[$pos+$nt]";
                }
		print OUT "\n";

		# print line with strand (always "+")
		$line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
		if($isfilenew) { print OUT "strand"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\t+";
                }
		print OUT "\n";

		# print 6 more lines here
		$line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "assembly#"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\t$genome_ver";
                }
                print OUT "\n";

                $line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "center"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\tNA";
                }
                print OUT "\n";

                $line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "protLSID"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\tNA";
                }
                print OUT "\n";

                $line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "assayLSID"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\tNA";
                }
                print OUT "\n";

                $line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "panelLSID"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\tNA";
                }
                print OUT "\n";

                $line_old = "";
                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
                if($isfilenew) { print OUT "QCcode"; }
                for($pos=0;$pos<$np;$pos++)
                {
                        print OUT "\tNA";
                }
                print OUT "\n";

		# print genotype lines
		my $allit = 0;
		for(my $it=0;$it<$nt;$it++)
		{
			$allit += length($data[$it]); #get an array of ascii values, only every $enc is printable as character
		}
		my $curit = 0;
		my $pstep = 0;
		if($progress>0){$pstep = int($allit/$progress);}
		for(my $it=0;$it<$nt;$it++)
		{
			$line_old = "";
	                if(! $isfilenew) { $line_old =<IN>; chomp $line_old; print OUT $line_old;}
			if($isfilenew) { print OUT "$data[$it+$n_taxa]"; }   # taxon header
			my @arr = split '', $data[$it]; #get an array of ascii values, only every $enc is printable as character
                       	for(my $j=0; $j<=$#arr; $j++)   # this loop should be over positions
                       	{
				my $outchar = $arr[$j];
                                if($outformat eq "NUMERIC") 
				{ 
					my @alleles = split "/", $data[$j+$n_allele];
					$outchar = allele_encode($arr[$j], @alleles); 
				}
                               	if($j % $enc == 0)
                               	{
                                       	print OUT "\t$outchar";
                               	}
                               	else
                               	{
                                       	print OUT "\t" . ord($outchar);
                               	}
				if($progress>0)
				{
					if($curit%$pstep==0)
					{
						my $ppp = int(1000*$curit/$allit)/10;
						$flush->("$curit/$allit $ppp\%");
					}
				}
				$curit++;
                        }
                       	print OUT "\n";    # end of the row
		}
		if($progress>0){$flush->("$curit/$allit 100.0\%");}
		close IN;
		close OUT;

		`mv ${outfname}_append ${outfname}`;

	}  # end of orientation selection

}

sub USERPASS
{
	my $self = shift;
	my $uname = shift;
	my $oldpass = shift;
	my $newpass = shift;

	$self->{ERR} = "";
	if($uname eq "")
	{
		$self->{ERR} = "user name is empty";
		return;
	}
	if($oldpass eq "")
	{
		$self->{ERR} = "old password is empty";
		return;
	}
	if($newpass eq "")
	{
		$self->{ERR} = "new password is empty";
		return;
	}
	$| = 1;
	my $socket = $self->open();
	print  $socket "USERPASS\n$uname\n$oldpass\n$newpass\n\n";
	
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
	return;
}

sub LOGIN
{
	my $self = shift;
	my $uname = shift;
	my $pass = shift;

	$self->{ERR} = "";
	if($uname eq "")
	{
		$self->{ERR} = "user name is empty";
		return;
	}
	if($pass eq "")
	{
		$self->{ERR} = "password is empty";
		return;
	}
	$| = 1;
	my $socket = $self->open();
	print  $socket "LOGIN\n$uname\n$pass\n\n";
	
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
	return @data;
}

sub ADMINCMD
{
	(my $self, my $password, my $command, my @arg) = @_;

	$self->{ERR} = "";
	if($password eq "" && $command ne "LISTALL")
	{
		$self->{ERR} =  "password is empty";
		return;
	}
	if($command eq "")
	{
		$self->{ERR} =  "command is empty";
		return;
	}
	if(index("|MOUNT|UMOUNT|INDEX|AFLIST|USERADD|USERDEL|USERACC|ULIST|LISTALL|", "|" . $command . "|") == -1)
	{
		$self->{ERR} =  "invalid command";
		return;
	}
	my $cmdtxt = "$command\n$password\n";
	if($command eq "LISTALL"){$cmdtxt = "$command\n";}
	for(my $i=0; $i<=$#arg; $i++)
	{
		$cmdtxt .= $arg[$i] . "\n";
	}
	$| = 1;
	my $socket = $self->open();
	print  $socket "$cmdtxt\n";
	
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
	return @data;
}

1;
__END__

=head1 NAME

CBSU::hdf5 - A module to access CBSU hdf5 server

=head1 SYNOPSIS

# Implemented hdf5 server functions:
# FLIST		list all mounted hdf5 files
# PLIST		list all available projects
# FINFO		information about mounted file
# PINFO		information about available project
# TABLE		query supporting data tables: positions, markers, taxa or alleles
# QUERY		query main data array
# USERPASS	change user password
# ADMINCMD	execute administrative command (see hdf5 server command reference)
#         	available admin commands: MOUNT, UMOUNT, INDEX, AFLIST, USERADD, USERDEL, USERACC, ULIST


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
	'position_type' => "index",	#type of the position index: 'index' - index in the array, 'value' - value of the position in positions array
	'starting_position' => $starting_index,	#starting index of the table
	'ending_position' => $ending_index	#ending index of the table
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



=head1 DESCRIPTION

This module is a proxy for using CBSU hdf5 server. All non-administrative functions are implemented. See SYNOPSIS for more information on syntax.

=head1 AUTHOR

Jaroslaw Pillardy, jp86@cornell.edu

=head1 SEE ALSO

perl(1).

=cut

