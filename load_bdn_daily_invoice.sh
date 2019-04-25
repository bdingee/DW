#!/bin/sh
##############################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/load_bdn_daily_invoice.sh,v 1.1 2011/07/21 18:29:52 iciombor Exp $
# $Revision: 1.1 $
# $Name:  $
# Shell script : load_bdn_daily_invoice.sh
#
# Description  : This script unzips the Daily Invoice file sent by BDN. The data is loaded into t tables for processing
# Files: 
#
#      1. CTL -->       t_bdn_customer.ctl 
#                       t_bdn_invoice.ctl
#                       t_bdn_chargeback.ctl
#
#      2. INPUT -->     DI_DGO_20110504_CUS.zip ???
#
#
#
#
#
#############################################################################

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=70057


# include common shell functions
. $DW_BIN/dw_common_scripts.sh

# setup runtime environment for subject
dw_setup_env $subject
dw_check_for_error -s "dw_setup_env" -e "$?"


#------------------------------------------------
# END - REQUIRED CODE
#------------------------------------------------

#
# start batch
#
dw_start_run_shell $batch_key
dw_check_for_error -s "dw_start_run_shell" -e "$?"

USER="DGO_FTP"
PASSWORD="bmt3sN_w"
SITE="io.bdn.com"
SUBDIR="out"


# Go to the BDN inbound directory
cd $DW_DATA_INBOUND_BDN

# If the old copy of the input exists delete that
# for now  rm -f DI_DGO_*.zip
# for now  rm -f DI_DGO_*.txt

# get the input file from BDN ftp server
echo "------------- ftp the input file DI_DGO_*.ZIP from BDN ftp server"
echo "          Start: `date`"

# for now  ftp -nvi io.bdn.com <<EOF
# for now     user DGO_FTP bmt3sN_w
# for now     binary
# for now     cd out
# for now     get DI_DGO_20110504_CUS.zip
# for now     quit
# for now  EOF

# dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c get "$file"

echo "ftp completed........ End: `date`"
echo " "


# unzip the input file, concatenate and rename 

echo "------------- unzipping and renaming the input files from .... "
echo "          Start: `date`"

# for now - need to check this  unzip -o DI_DGO_20110504_CUS.zip -x *.txt 
# for now  dw_check_for_error -s "unzip" -e "$?"

#Put all filenames in uppercases.
#this code will convert all the lowercase file or mixedcase filenames to uppercase names
# for now  for i in *
# for now  do
# for now     j=`echo $i | tr '[a-z]' '[A-Z]'`
# for now     if [ "$i" != "$j" ]
# for now     then
# for now        mv $i $j
# for now     fi
# for now  done


continue_load=1

# If all the input files are found

if [ $continue_load -gt 0 ] 
then

# renaming TXT files
   echo "Renaming TXT files"

   echo "  -- Renaming *_CUS.txt => t_bdn_customer.txt"
   echo "  -- Renaming *_CHG.txt => t_bdn_chargeback.txt"
   echo "  -- Renaming *_INV.txt => t_bdn_invoice.txt"
 # for now  mv 0007000890_20110428_CUS.txt  t_bdn_customer.txt
 # for now     mv 0007000890_20101201_CHG.txt  t_bdn_chargeback.txt
 # for now  mv 0007000890_20101231_INV.txt  t_bdn_invoice.txt


   if ! test -f t_bdn_customer.txt || 
      ! test -f t_bdn_chargeback.txt || 
      ! test -f t_bdn_invoice.txt
   then   
  
      echo " All the text files required for processing are not available"
      continue_load=0

     # rm -f $DW_DATA_INBOUND_BDN/*.txt
     # rm -f $DW_DATA_INBOUND_BDN/*.zip

      dw_finish_run_shell -f -r $DW_RUN_KEY $DW_BATCH_KEY
      dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   
      exit $DW_RETURN_FAILURE

   fi
fi


# If all the required input files found after re-naming and combining

if [ $continue_load -gt 0 ] 
then

# move to the work directory
   cd $DW_DATA_WORK

   echo "Processing file: t_bdn_customer.txt"
   echo " "
   echo "---------------  sqlload the data file into t_bdn_customer table"
   echo "          Start: `date`"

   dw_sqlload -c "$DW_SUBJECT_CTL/t_bdn_customer.ctl" \
              -r "$DW_RUN_KEY"                         \
                 "$DW_DATA_INBOUND_BDN/t_bdn_customer.txt"
   dw_check_for_error -s "dw_sqlload" -e "$?"
   
   echo "                      End: `date`"
   echo " "

  
echo "Processing file: t_bdn_chargeback.txt"
   echo " "
   echo "---------------  sqlload the data file into t_bdn_chargeback table"
   echo "          Start: `date`"

   dw_sqlload -c "$DW_SUBJECT_CTL/t_bdn_chargeback.ctl" \
              -r "$DW_RUN_KEY"                         \
                 "$DW_DATA_INBOUND_BDN/t_bdn_chargeback.txt"
   dw_check_for_error -s "dw_sqlload" -e "$?"
   
   echo "                      End: `date`"
   echo " "




   echo "Processing file: t_bdn_invoice.txt"
   echo " "
   echo "---------------  sqlload the data file into t_bdn_chargeback table"
   echo "          Start: `date`"

   dw_sqlload -c "$DW_SUBJECT_CTL/t_bdn_invoice.ctl" \
              -r "$DW_RUN_KEY"                         \
                 "$DW_DATA_INBOUND_BDN/t_bdn_invoice.txt"
   dw_check_for_error -s "dw_sqlload" -e "$?"
   
   echo "                      End: `date`"
   echo " "

else
   echo " All the txt and zip data files required for processing are not available"
   # rm -f $DW_DATA_INBOUND_BDN/*.txt
   # rm -f $DW_DATA_INBOUND_BDN/*.zip

   echo " Finish Batch Failed"
   dw_finish_run_shell -f -r $DW_RUN_KEY $DW_BATCH_KEY
   dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   
   exit $DW_RETURN_FAILURE
  
fi

#
# finish batch - SUCCESSFUL
#

# removing input files as they will be re-created in next run
# rm -f $DW_DATA_INBOUND_BDN/*.txt
# rm -f $DW_DATA_INBOUND_BDN/*.zip

echo " Finish Batch Success"
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"

exit $DW_RETURN_SUCCESS

