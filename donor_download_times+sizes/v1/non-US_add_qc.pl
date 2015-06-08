# Take values from all projects to generate data for average download times and sizes per donor
# Note: US projects have suspect timing metrics (qc_timing values are large negative numbers, unknown validity of other timing data)
#       - run with all data, add qc timing, do check to see if it's negative, if yes, exclude it
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

open IN, "<non-US_projects.jsonl" or die;

open(my $fh, '>', 'non-US_projects_with_qc.txt');

print $fh "TYPE\tMERGE_TIME\tBWA_TIME\tDOWN_TIME\tQC_TIME\n";

while(<IN>) {
  chomp;
  my $json = decode_json($_);

  #print Dumper $json;

  if (defined ($json->{normal_specimen}{alignment}{timing_metrics})) {

    my $norm_merge_timing_seconds = 0;
    my $norm_bwa_timing_seconds = 0;
    my $norm_download_timing_seconds = 0;
    my $norm_qc_timing_seconds = 0;

    foreach my $hash (@{$json->{normal_specimen}{alignment}{timing_metrics}}) {
      #print Dumper $hash;
      $norm_merge_timing_seconds = $hash->{metrics}{merge_timing_seconds};
      if ($hash->{metrics}{bwa_timing_seconds} > $norm_bwa_timing_seconds) { $norm_bwa_timing_seconds = $hash->{metrics}{bwa_timing_seconds}; }
      $norm_download_timing_seconds = $hash->{metrics}{bwa_timing_seconds};
      if ($hash->{metrics}{qc_timing_seconds} > $norm_qc_timing_seconds) { $norm_qc_timing_seconds = $hash->{metrics}{qc_timing_seconds}; }
    }

    # print to file
    print $fh "NORM\t$norm_merge_timing_seconds\t$norm_bwa_timing_seconds\t$norm_download_timing_seconds\t$norm_qc_timing_seconds\n";

  }

  #if (defined ($json->{aligned_tumor_specimens}) ) {

    foreach my $tumor (@{$json->{aligned_tumor_specimens}}) {

      my $tumor_merge_timing_seconds = 0;
      my $tumor_bwa_timing_seconds = 0;
      my $tumor_download_timing_seconds = 0;
      my $tumor_qc_timing_seconds = 0;

      #print Dumper($tumor);

      foreach my $hash (@{$tumor->{alignment}{timing_metrics}}) {
        #print Dumper $hash;
        $tumor_merge_timing_seconds = $hash->{metrics}{merge_timing_seconds};
        if ($hash->{metrics}{bwa_timing_seconds} > $tumor_bwa_timing_seconds) { $tumor_bwa_timing_seconds = $hash->{metrics}{bwa_timing_seconds}; }
        $tumor_download_timing_seconds = $hash->{metrics}{bwa_timing_seconds};
        if ($hash->{metrics}{qc_timing_seconds} > $tumor_qc_timing_seconds) { $tumor_qc_timing_seconds = $hash->{metrics}{qc_timing_seconds}; }
      }

      print $fh "TUMOR\t$tumor_merge_timing_seconds\t$tumor_bwa_timing_seconds\t$tumor_download_timing_seconds\t$tumor_qc_timing_seconds\n";
      # show progress
      print "."

    }

#  }
print "\n"
}

close IN;
