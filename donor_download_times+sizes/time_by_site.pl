# Take values from ICGC projects to generate data for average download times and sizes per donor, by site
# Modified from Brian's code https://github.com/briandoconnor/my-sandbox/blob/92ee52fa9816593a23c8210eb7dd17cfebf4a9c1/20150520_analysis_for_EBI_runtimes/generate-stats.pl#L21
use strict;
use warnings;
use JSON;
use Data::Dumper;

my $project_code = shift;

# get the data file
if (!-e "donor_p_150520020206.jsonl") {
  die if (system("wget http://pancancer.info/gnos_metadata/2015-05-20_02-02-06_UTC/donor_p_150520020206.jsonl.gz"));
  die if (system("gunzip donor_p_150520020206.jsonl.gz"));
}

open IN, "<ICGC_only_donor_p_150520020206.jsonl" or die;

open(my $fh, '>', 'data/ICGC_time_by_site.txt');

print $fh "TYPE\tPROJECT_CODE\tDONOR_ID\tMERGE_TIME\tBWA_TIME\tDOWN_TIME\tQC_TIME\tGNOS_REPO\n";

while(<IN>) {
  chomp;
  my $json = decode_json($_);

  my $project_code = $json->{dcc_project_code};
  my $donor_id = $json->{submitter_donor_id};
  #print Dumper $json;

  if (defined ($json->{normal_specimen}{alignment}{timing_metrics})) {

    my $sample_id = $json->{normal_specimen}->{submitter_sample_id};

    my $normal_gnos_repo = "";
    $normal_gnos_repo = $json->{normal_specimen}{'gnos_repo'};

    my $norm_merge_timing_seconds = 0;
    my $norm_bwa_timing_seconds = 0;
    my $norm_download_timing_seconds = 0;
    my $norm_qc_timing_seconds = 0;

    foreach my $hash (@{$json->{normal_specimen}{alignment}{timing_metrics}}) {
      #print Dumper $hash;
      $norm_merge_timing_seconds += ! defined $hash->{metrics}{merge_timing_seconds} || $hash->{metrics}{merge_timing_seconds} <=0 ? 0 : $hash->{metrics}{merge_timing_seconds} ;

      $norm_bwa_timing_seconds += ! defined $hash->{metrics}{bwa_timing_seconds} || $hash->{metrics}{bwa_timing_seconds} <= 0 ? 0 : $hash->{metrics}{bwa_timing_seconds} ;
      
      $norm_download_timing_seconds += ! defined  $hash->{metrics}{download_timing_seconds} || $hash->{metrics}{download_timing_seconds} <= 0 ? 0 : $hash->{metrics}{download_timing_seconds} ;

      $norm_qc_timing_seconds += ! defined $hash->{metrics}{qc_timing_seconds} || $hash->{metrics}{qc_timing_seconds} <= 0 ? 0 : $hash->{metrics}{qc_timing_seconds} ;

    }

    # print to file
    print $fh "NORM\t$project_code\t$donor_id\t$norm_merge_timing_seconds\t$norm_bwa_timing_seconds\t$norm_download_timing_seconds\t$norm_qc_timing_seconds\t$normal_gnos_repo\n";

  }

  #if (defined ($json->{aligned_tumor_specimens}) ) {


    foreach my $tumor (@{$json->{aligned_tumor_specimens}}) {

      my $gnos_repo = "";
      my $tumor_merge_timing_seconds = 0;
      my $tumor_bwa_timing_seconds = 0;
      my $tumor_download_timing_seconds = 0;
      my $tumor_qc_timing_seconds = 0;

      $gnos_repo = $tumor->{'gnos_repo'};

      #print Dumper($tumor);

      foreach my $hash (@{$tumor->{alignment}{timing_metrics}}) {
        #print Dumper $hash;
        $tumor_merge_timing_seconds += ! defined $hash->{metrics}{merge_timing_seconds} || $hash->{metrics}{merge_timing_seconds} <=0 ? 0 : $hash->{metrics}{merge_timing_seconds} ;
 
        $tumor_bwa_timing_seconds += ! defined $hash->{metrics}{bwa_timing_seconds} || $hash->{metrics}{bwa_timing_seconds} <= 0 ? 0 : $hash->{metrics}{bwa_timing_seconds} ;

        $tumor_download_timing_seconds += ! defined $hash->{metrics}{download_timing_seconds} || $hash->{metrics}{download_timing_seconds} <= 0 ? 0 : $hash->{metrics}{download_timing_seconds} ;

        $tumor_qc_timing_seconds += ! defined $hash->{metrics}{qc_timing_seconds} || $hash->{metrics}{qc_timing_seconds} <= 0 ? 0 : $hash->{metrics}{qc_timing_seconds} ;

      }

      print $fh "TUMOR\t$project_code\t$donor_id\t$tumor_merge_timing_seconds\t$tumor_bwa_timing_seconds\t$tumor_download_timing_seconds\t$tumor_qc_timing_seconds\t$gnos_repo\n";
      # show progress
      print "."

    }

#  }
print "\n"
}

close IN;
