#!/bin/sh
########################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/ftp_axces_depletions.sh,v 1.6 2009/02/24 23:40:04 abvita Exp $
# $Revision: 1.6 $
# $Name:  $
#
# Description  : This script ftps the DGUSA VIP Depletions data files
#                and trigger file to the Axces inbound directory.
#
# Files: 
#      1. TRIG FILE --> GODEPL.TRIG.gz
#      2. DATA FILE --> DEPL.NYYYYMMDD.gz - Normal daily extract from the beginning of the current Diageo Fiscal Year(ex.20080701)
#                                           to current date 
#                       DEPLFX8529.NYYYYMMDD.gz -  Fix file 
#                       DEPLFX8195.NYYYYMMDD.gz -  Zero fix file 
#
########################################################################

#set -x

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=71415


# include common shell functions
. $DW_BIN/dw_common_scripts.sh

# setup runtime environment for subject
dw_setup_env $subject
dw_check_for_error -s "dw_setup_env" -e "$?"

# move to the work directory
cd $DW_DATA_WORK

#------------------------------------------------
# END - REQUIRED CODE
#------------------------------------------------


#
# start batch
#
dw_start_run_shell $batch_key
dw_check_for_error -s "dw_start_run_shell" -e "$?"


USER="gusout"
PASSWORD="Y7aq#29$"
SITE="sftp.vtinfo.com"

load_failure=0
tmpf=`mktemp -d "$DW_DATA_WORK"`

#
# cd to the DEPLETIONS inbound directory
#
cd $DW_DATA_INBOUND_AXCES


#
#
# loop checking for the trigger file until it appears
# or 10 min (15sec * 40) has elapsed
#
n=0
trigger="N"
#######################rad_file_count=0

echo "Waiting for trigger file"

while [ $n -lt 16 ]
do
   dw_ftp -u $USER -p $PASSWORD -d $SITE -c delete "GODEPL.TRIG.gz" >/dev/null

   if [ "$?" -ne 0 ]
   then
      n=`expr $n + 1`
      sleep 120
   else
      trigger="Y"
      break
   fi
done

if [ "$trigger" = "N" ]
then
   # don't treat no trigger file as a failure
   echo  "ftp_axces_depletions: No Trigger File: GODEPL.TRIG.gz" 
   load_failure=0
else
   #
   # get a list of the zipped data files
   #
   echo "Getting list of available data files"

   dw_ftp -u $USER -p $PASSWORD -d $SITE -c nlist "DEPL*.N????????.gz $tmpf" >/dev/null

   if [ "$?" -ne 0 -o `cat $tmpf | wc -l` -eq 0 ]
   then
      rm -f $tmpf

      # no file found
      echo "No Axces Depletion files on ftp server to process.  Exiting..."

      # not having datafiles on ftp server is not an error. Proceed normaly and create a trigger file at end
      load_failure=0

   else
      for file in `cat $tmpf`
      do
         echo "Getting: $file"

         dw_ftp -u $USER -p $PASSWORD -d $SITE -c get "$file"

         if [ "$?" -ne 0 ]
         then
            # error ftp'ing
            error_msg="DWC-00001: get $file failed"
            echo "$error_msg"
            dw_create_error_shell -s "Get Data File: $file" -m "$error_msg"
            load_failure=1
            break
         fi

         echo "Renaming on FTP server"

         dw_ftp -u $USER -p $PASSWORD -d $SITE -c rename "$file dgo${file}"

         if [ "$?" -ne 0 ]
         then
            # error ftp'ing
            error_msg="DWC-00001: rename $file failed"
            echo "$error_msg"
            dw_create_error_shell -s "Rename Data File: $file" -m "$error_msg"
            load_failure=1

            # remove local lfile
            rm -f $file

            break
         fi
      echo " "
      done
   fi
fi

#
# always create trigger files when done
#
echo "Create trigger file"
touch $DW_DATA_INBOUND_AXCES/GODEPL.TRIG.gz


#
# finish batch - SUCCESSFUL
#
if [ "$load_failure" -eq 0 ] 
then
   rm -f $tmpf

   dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
   dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   exit $DW_RETURN_SUCCESS
else
   dw_finish_run_shell -f -r $DW_RUN_KEY $DW_BATCH_KEY
   dw_check_for_error -s "dw_finish_run_shell" -e "$?"
   exit $DW_RETURN_FAILURE
fi
