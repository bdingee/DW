#!/bin/sh
########################################################################
#$Header: /dwa/cvsrepo/dev/depletion/bin/dgocust.sh,v 1.5 2013/04/02 04:05:21 abvita Exp $
#$Revision: 1.5 $
#$Name:  $
# 
#  Extract customer shipment equals depletion data for BDN outbound file
#
########################################################################
#set -x 

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=502


# include common shell functions
. $DW_BIN/dw_common_scripts.sh

# setup runtime environment for subject
dw_setup_env $subject
dw_check_for_error -s "dw_setup_env" -e "$?"

# move to the work directory
cd $DW_DATA_WORK
export TMPDIR=$DW_DATA_WORK   # TMPDIR need to set to $DW_DATA_WORK because default is /tmp

#------------------------------------------------
# END - REQUIRED CODE
#------------------------------------------------


#
# start batch
#
dw_start_run_shell $batch_key
dw_check_for_error -s "dw_start_run_shell" -e "$?"

#FTP Site Info
USER="DGO_FTP"
PASSWORD="bmt3sN_w"
SITE="io.bdn.com"
SUBDIR="in"
ftp_failure=0

echo "---------------  sqlplus - run extract process"
echo "---------------  Start: `date`"
echo

sql="@$DW_SUBJECT_SQL/dgocust.sql"
outfile=`mktemp -p $DW_DATA_WORK`
dw_check_for_error -s "mktemp" -m "Make Tempfile Failed" -e "$?"
   
dw_sqlplus -o $outfile "$sql"
dw_check_for_error -s "dw_sqlplus" -e "$?"

# cleanup file here
echo "---------------  Clean up output file"
echo

cleanfile="dgocust.txt"
grep '^[[:space:]]*#' $outfile | sed -e 's/^[[:space:]]*#//' > $cleanfile
dw_check_for_error -s "cleanup" -m "Cleaning of file failed" -e "$?"
   
# compress it
echo "---------------  compress the output file"
echo

compress -f $cleanfile
dw_check_for_error -s "compress" -m "Compress of file failed" -e "$?"

# move file
echo "---------------  move the output file to the outbound dir"
echo

mv -f "$cleanfile.Z" $DW_DATA_OUTBOUND_BDN
dw_check_for_error -s "move" -m "Move of file failed" -e "$?"
   
# cleanup
echo "--------------- cleanup the temporary file"
echo
rm -f $outfile

# check trigger
echo "--------------- make sure that trigger file exists"
if [ ! -f $DW_DATA_OUTBOUND_BDN/sentit ] 
then
 
     echo "   trigger file sentit is missing, lets create it"
     touch $DW_DATA_OUTBOUND_BDN/sentit
else
     echo "   trigger file ok"
fi    

cd $DW_DATA_OUTBOUND_BDN
# ftp
echo "--------------- FTP the compressed data file to BDN server"
echo
dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c put "$cleanfile.Z"
if [ "$?" -ne 0 ]
then
   # error ftp'ing
   error_msg="DWC-00001: put $file failed"
   echo "$error_msg"
   dw_create_error_shell -s "put Data File: $file" -m "$error_msg"
   ftp_failure=1
fi

# ftp
echo "--------------- FTP the trigger file to BDN server"
echo
dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c put "sentit"
if [ "$?" -ne 0 ]
then
   # error ftp'ing
   error_msg="DWC-00001: put sentit failed"
   echo "$error_msg"
   dw_create_error_shell -s "put trigger File: sentit" -m "$error_msg"
   ftp_failure=1
fi

#
# finish batch - SUCCESSFUL
if [ "$ftp_failure" -eq 0 ] 
then
   echo "-------------- finish extract process"
   echo "-------------- End: `date`"
   echo 
   dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
   dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   exit $DW_RETURN_SUCCESS
else
   echo "-------------- process finished with errors in FTP"
   echo "-------------- End: `date`"
   echo 
   dw_finish_run_shell -f -r $DW_RUN_KEY $DW_BATCH_KEY
   dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   exit $DW_RETURN_FAILURE
fi
