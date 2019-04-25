#!/bin/sh
########################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/ftp_bdn_wholesaler_links.sh,v 1.4 2013/10/10 00:58:43 mfarina Exp $
# $Revision: 1.4 $
# $Name:  $
#
# Description  : This script ftps the BDN Wholesales Links files
#                to the bdn archive directory.
#
# Files: 
#      1. TRIG FILE --> no trigger file
#      2. DATA FILE --> bdnwhsprd*.Z
#
########################################################################

#set -x

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=71405


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


USER="DGO_FTP"
PASSWORD="bmt3sN_w"
SITE="io.bdn.com"
SUBDIR="out"

load_failure=0
tmpf=`mktemp -p "$DW_DATA_WORK"`



#
# cd to the inbound directory
#

   cd $DW_DATA_INBOUND_BDN
   
   #
   # get a list of the zipped data files
   #
   echo "Getting list of available data files"

   dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c nlist "bdnwhsprd*.Z $tmpf" >/dev/null

   if [ "$?" -ne 0 -o `cat $tmpf | wc -l` -eq 0 ]
   then
      rm -f $tmpf

      # no file found
      echo "No BDN Wholesales Links files on ftp server to process.  Exiting..."

      # not having datafiles on ftp server is not an error. Proceed normaly and create a trigger file at end
      load_failure=0

   else
      for file in `cat $tmpf`
      do
         echo "Getting: $file"

         dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c get "$file"

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

         dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -c rename "$file dgo${file}"

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
