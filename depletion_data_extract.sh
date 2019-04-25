#!/bin/sh
#############################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/depletion_data_extract.sh,v 1.5 2013/04/02 04:03:27 abvita Exp $
# $Revision: 1.5 $
# $Name:  $
#
# Description  : This script is used for depletion data extract.
#
# F Tables:
#
#############################################################################
#set -x
#---------------------------------------------------------------------------
# Required Code
#---------------------------------------------------------------------------
FY=$1
if [ -z "$FY" ]
then
 #integer MON=`date +%m`
 MON=`date +%m`
 if  (( $MON <= 7 ))
 then
      FY=`date +%Y`
 else
      FY=`date +%Y`
      (( FY += 1 ))
 fi
fi

subject=depletion
batch_key=512

# include common shell functions
. $DW_BIN/dw_common_scripts.sh

# Set the runtime environment for the subject area DEPLETION
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

  outfile=`mktemp -p $DW_DATA_WORK`

      if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
      then
         dw_check_for_error -s "mktemp" -m "mktemp failed" -e "$?"
      fi

# Execute the SQL for depletion extract

   echo "---------------  sqlplus - run load process for depletion for FY=$FY"
   echo "                    Start: `date`"

sql="@$DW_SUBJECT_SQL/depletion_data_extract.sql
                                 $FY"

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

      cleanfile="depletion_data_extract_$file_date"

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

      zipfile="DEPLETION_DATA_EXTRACT_$file_date"

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
# dce_depl_list is defined in .mailrc file

   echo "---------------  send email."
   echo "                    Start: `date`"
   FDATE=`date +%Y%m%d`
   FROM='NADW.do.not.reply'
   sSUB='depletion Data Extract'
   ##uuencode $zipfile.zip < $zipfile.zip | mailx -s "$sSUB" -r $FROM  dce_depl_list 
   uuencode $zipfile.zip < $zipfile.zip | mailx -s "$sSUB" dce_depl_list 

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
