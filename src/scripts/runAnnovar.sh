#!/bin/bash
ANNOVARDIR=$1
ANNOVARDB=$2
BUILDVER=$3
INPUTVCF=$4
OUTPUTVCF=$5
WORKDINGDIR=$6
cd $WORKDINGDIR
$ANNOVARDIR/convert2annovar.pl -format vcf4 $INPUTVCF > $(basename $INPUTVCF .vcf).avinput
$ANNOVARDIR/annotate_variantion.pl -downdb -buildver $BUILDVER -webfrom annovar refGene $ANNOVARDB
$ANNOVARDIR/annotate_variantion.pl -out $(basename $INPUTVCF .vcf) -build $BUILDVER $(basename $INPUTVCF .vcf).avinput $ANNOVARDB
exit 0
