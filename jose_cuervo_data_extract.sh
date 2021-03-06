#!/bin/sh
#############################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/jose_cuervo_data_extract.sh,v 1.3 2009/10/08 19:28:16 mfarina Exp $
# $Revision: 1.3 $
# $Name:  $
#
# Description  : This script is used for jose_cuervo data extract.
#
# F Tables:
#
#############################################################################
#set -x
#---------------------------------------------------------------------------
# Required Code
#---------------------------------------------------------------------------

EX_MONTH=$1
if [ -z "$EX_MONTH" ]
then
   START_MONTH=`date +%Y%m`
   START_MONTH=$((START_MONTH -1))
else
    START_MONTH=$EX_MONTH
fi

subject=depletion
batch_key=515

# include common shell functions
. $DW_BIN/dw_common_scripts.sh

# Set the runtime environment for the subject area ACCTSOLD
dw_setup_env $subject
dw_check_for_error -s "dw_setup_env" -e "$?"

cd $DW_DATA_WORK

#---------------------------------------------------------------------------
# END - Required Code
#---------------------------------------------------------------------------

#
# --------------- Start Batch ----------------
#
dw_start_run_shell $batch_key
dw_check_for_error -s "dw_start_run_shell" -e "$?"

  outfile=`mktemp -d $DW_DATA_WORK`

      if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
      then
         dw_check_for_error -s "mktemp" -m "mktemp failed" -e "$?"
      fi

# Execute the SQL for jose_cuervo extract

   echo "---------------  sqlplus - run load process for jose_cuervo "
   echo "                    Start: `date`"

sql="@$DW_SUBJECT_SQL/jose_cuervo_extract.sql 
                                 $START_MONTH"
dw_sqlplus -o $outfile "$sql"

      if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
      then
         dw_check_for_error -s "dw_sqlplus" -e "$?"
      fi

echo "                End: `date`"
echo " "

# cleanup file here
      echo "---------------  clean - Clean up output file"
      echo "                    Start: `date`"

      file_date=`date +%Y%m`

      cleanfile="JOSE_CUERVO_EXTRACT_$file_date"

      #
      # only get lines that begin with a #, then remove the #
      # make sure any " inside " are ""
      #
      grep '^[[:space:]]*#' $outfile | sed -e 's/^[[:space:]]*#//' -e 's/\([^,]\)"\([^,]\)/\1""\2/' > $cleanfile.csv

      if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
      then
         dw_check_for_error -s "cleanup" -m "Cleaning of file failed" -e "$?"
      fi

      echo "                      End: `date`"
      echo " "

      zipfile="JOSE_CUERVO_EXTRACT_$file_date"

# zip file
      echo "---------------  create the zip file $cleanfile.zip"
      echo "                    Start: `date`"
      zip -l $zipfile.zip $cleanfile.csv

      if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
      then
         dw_check_for_error -s "Zip_File" -m "Zip file failed" -e "$?"
      fi

      echo "                      End: `date`"
      echo " "


# Mail from Commerical_DB_Team to the user regarding Data extract
   echo "---------------  send email."
   echo "                    Start: `date`"
   FDATE=`date +%Y%m%d`
   FROM='Commerical_DB_Team'
   sSUB='jose_cuervo Data Extract'
   uuencode $zipfile.zip < $zipfile.zip | mailx -s "$sSUB" -r $FROM  "itdept@josecuervo.org"

   dw_check_for_error -s "send_email" -e "$?"

   echo "                      End: `date`"
   echo " "

   # cleanup

     rm -f $outfile
     rm -f $cleanfile.csv
     rm -f $zipfile.zip


#
# ---------------- End Batch --------------------
#

# finish batch - SUCCESSFUL
#
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"

exit $DW_RETURN_SUCCESS
