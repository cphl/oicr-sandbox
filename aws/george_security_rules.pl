#!/usr/bin/perl

use JSON qw( decode_json );

my $json = do { local $/; <STDIN> };

my $decoded = decode_json($json);

my @secgrp = @{ $decoded->{'SecurityGroups'} };
foreach my $f ( @secgrp ) {
 $desc=$f->{"Description"};
 $group=$f->{"GroupName"};

 my @ipperm = @{ $f->{'IpPermissions'} };

 foreach my $g ( @ipperm ) {
  $toport=$g->{'ToPort'};
  $fromport=$g->{'FromPort'};
  $proto=$g->{'IpProtocol'};

  my @cidr = @{ $g->{'IpRanges'} };
  foreach my $h ( @cidr ) {
   $cidr=$h->{'CidrIp'};

    print "Group Name : $group\n";
    print "Description: $desc\n";
    if ($proto==-1) {
     print "any IP traffic, from source $cidr\n";
    } else {
     if ($fromport ne $toport) {
      print "ports $fromport:$toport/$proto, from source $cidr\n";
    } else {
     print "port $fromport/$proto, from source $cidr\n";
    }
   }

   print "\n";

  }
 }
}
