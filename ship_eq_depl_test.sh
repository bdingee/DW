#!/bin/sh
########################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/ship_eq_depl_test.sh,v 1.4 2013/05/24 01:51:50 iciombor Exp $
# $Revision: 1.4 $
# $Name:  $
#
# Description  : This script creates a depletion input file for the
#                normal BDN Depletion load.  This data is the ship=depl
#                data for all customers that are flagged as ship=depl.
#                (e.q. depletion_reporting_method=Y)
#                This process support restatement.
#
#                Parameter: YYYYMM - year/month to restate ship=depl
#
#
# MODIFICATION HISTORY
# Person      Date     Comments
# ---------   -------- ----------------------------------------
# B. Pinto    11/03/03 New
########################################################################
#set -x

N_MONTHS=$1
if [ -z "$N_MONTHS" ]
then
   N_MONTHS=1
fi


#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=505


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


outfile=`mktemp -p $DW_DATA_WORK`
echo "outfile = " $outfile

if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
then
   dw_check_for_error -s "mktemp" -m "mktemp failed" -e "$?"
fi


   
sql="@$DW_SUBJECT_SQL/ship_eq_depl_iwonka.sql
	  $N_MONTHS"


echo "---------------  sqlplus - run extract process"
echo "                    Start: `date`"
dw_sqlplus -o $outfile "$sql"

if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
then
   dw_check_for_error -s "dw_sqlplus" -e "$?"
fi

echo "                      End: `date`"
echo " "


#
# remove check for period closed
#
##
## check to see if the file should be processed - only if processing
## previous month only
##
#if [ "$N_MONTHS" -eq 1 ]
#then
#
#   grep "PERIOD OPEN DO NOT LOAD" $outfile > /dev/null
#
#   if [ $? -eq 0 ]
#   then
#      echo "Period is still open - skipping processing of data"
#
#      # cleanup
#      rm -f $outfile
#
#      dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
#      dw_check_for_error -s "dw_finish_run_shell" -e "$?"
#
#      exit $DW_RETURN_SUCCESS
#
#   elif [ $? -eq 1 ]
#   then
#      echo "Period has been closed - processing data..."
#
#   else
#      dw_check_for_error -s "grep - period" -m "grep of file failed" -e "$?"
#   fi
#
#fi


# cleanup file here
echo "---------------  clean - Clean up output file"
echo "                    Start: `date`"
cleanfile="shipeqdepl.`date +%Y%m%d_%H:%M:%S`"

echo "cleanfile = " $cleanfile

#
# only get lines that begin with a #, then remove the #
# make sure any " inside " are ""
#
grep '^[[:space:]]*#' $outfile | sed -e 's/^[[:space:]]*#//' -e 's/\([^,]\)"\([^,]\)/\1""\2/' > $cleanfile

if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
then
   dw_check_for_error -s "cleanup" -m "Cleaning of file failed" -e "$?"
fi

echo "                      End: `date`"
echo " "

   
# compress it
echo "---------------  compress - compress the output file"
echo "                    Start: `date`"
compress -f $cleanfile

if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
then
   dw_check_for_error -s "compress" -m "Compress of file failed" -e "$?"
fi

echo "                      End: `date`"
echo " "


# move file
echo "---------------  move - move the output file to the inbound dir"
echo "                    Start: `date`"
mv -f "$cleanfile.Z" $DW_DATA_INBOUND_SHIPDEPL

if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
then
   dw_check_for_error -s "move" -m "Move of file failed" -e "$?"
fi

echo "                      End: `date`"
echo " "

   
# cleanup
rm -f $outfile

#
# Now run the regular Depletion load
#
file="$DW_DATA_INBOUND_SHIPDEPL/$cleanfile.Z"

echo "---------------  sqlload the data file into the T table"
echo "                    Start: `date`"

dw_sqlload -c "$DW_SUBJECT_CTL/t_bdn_depletions.ctl" \
           -r "$DW_RUN_KEY"                               \
           "$file"
dw_check_for_error -s "dw_sqlload" -e "$?"
   
echo "                      End: `date`"
echo " "


echo "---------------  sqlplus - run load process"
echo "                    Start: `date`"

sql="f_bdn_depletions_pkg.load('"$file"', 12);"

dw_sqlplus -p "$sql"
dw_check_for_error -s "dw_sqlplus" -e "$?"

echo "                      End: `date`"
echo " "


echo "---------------  archive the data file"
echo "                    Start: `date`"

dw_archive_file -d -c "$file"
dw_check_for_error -s "dw_archive_file" -e "$?"

echo "                      End: `date`"
echo " "

echo "            End: `date`"
echo " "


#
# finish batch - SUCCESSFUL
#
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"

echo " "
echo " "
echo "**********************************************************************"
echo "---------------  Completed Successfully"
echo "**********************************************************************"

exit $DW_RETURN_SUCCESS
