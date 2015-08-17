#!/bin/bash
ANNOVARDIR=$1
ANNOVARDB=$2
BUILDVER=$3
INPUTVCF=$4
OUTPUT=$5
WORKDINGDIR=$6
cd $WORKDINGDIR
$ANNOVARDIR/convert2annovar.pl -format vcf4 $(basename $INPUTVCF) > $(basename $INPUTVCF .vcf).avinput
$ANNOVARDIR/annotate_variation.pl -downdb -buildver $BUILDVER -webfrom annovar refGene $ANNOVARDB
$ANNOVARDIR/annotate_variation.pl -out $OUTPUT -build $BUILDVER $(basename $INPUTVCF .vcf).avinput $ANNOVARDB
exit 0
