#!/bin/sh
############################################################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/load_dd_inventory.sh,v 1.3 2016/03/26 14:03:18 bdingee Exp $ 
# $Revision: 1.3 $
# $Name:  $
#
# Shell script : load_dd_inventory.sh
#
# Description  : This script takes the daily depletions inventory fact and populates t_, w_ and f_ tables
#
# Files: 
#
#      1. CTL -->  t_dd_inventory.ctl  --for fact data 
#
#      2. DATA FILE --> INVDA.NYYYYMMDD     -- fact data 
#                          
#      3. Trigger File  --> GODDINV.DAT.gz
#                       
############################################################################################################
#set -x

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=70275


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

ZIP_EXT=zip
ZIP_COMMAND="zip -mT"    ### gzip
UNZIP_COMMAND="unzip -o" ### gunzip

TRIGGER_FILE=GODDINV.DAT.zip

#
# wait for the trigger file to arrive
#
dw_wait_for_file -n 720 -d $DW_DATA_INBOUND_DEPLETION/$TRIGGER_FILE
dw_check_for_error -s "dw_wait_for_file" -e "$?"

fn_process_inventory_file () {

   cd $DW_DATA_INBOUND_DEPLETION
   fname=$1
   rstate=$2

   echo "      ---------------  Processing: $fname"

   echo "      ---------------  Unzip the file "
   INBOUND_FILE=`echo $fname | cut -f1-2 -d"."`
   eval "$UNZIP_COMMAND" $fname #### unzip $fname

   dw_check_for_error -s "$UNZIP_COMMAND" -m "$UNZIP_COMMAND $INBOUND_FILE failed" -e "$?"
   mv $fname `echo ${fname%/*}/done-${fname##*/}`

   echo "      ---------------  convert from DOS format to UNIX format."
   dos2unix $INBOUND_FILE
   echo "                       Start: `date`"
   echo "      ---------------  Validate file integrity"
   #file should have control rec + 2 (header and footer ) records
   controlrec=`tail -1 $INBOUND_FILE | cut -f4 -d"|"`
   recinfile=`wc -l $INBOUND_FILE | sed -e 's/^ *//' | cut -f1 -d" "`
   recinfile=`expr $recinfile - 2`

   if [ "$recinfile" -eq "$controlrec" ]
   then
      echo "      ---------------  sqlload the data file into the T table"
      echo "                       Start: `date`"

      dw_sqlload -c "$DW_SUBJECT_CTL/t_dd_inventory.ctl" \
                 -r "$DW_RUN_KEY"                           \
                    "$INBOUND_FILE"

      dw_check_for_error -s "dw_sqlload" -e "$?"
      echo "                         End: `date`"
      echo " "

      echo "      ---------------  sqlplus - run load process"
      echo "                       Start: `date`"

      sql="load_dd_inventory_pkg.load('"`basename $INBOUND_FILE`"', '"$rstate"');"
      dw_sqlplus -p "$sql"

      dw_check_for_error -s "dw_sqlplus" -e "$?"
      echo "                         End: `date`"
      echo " "

      echo "---------------  archive the data file"
      echo "                    Start: `date`"

      dw_archive_file -d -c "$INBOUND_FILE"
      dw_check_for_error -s "dw_archive_file" -e "$?"

      echo "                      End: `date`"
      echo " "
   else
      echo " "
      echo "         ---------------  Control Rec total does not match with total number of records in file (excluding header, footer)."
   fi
}

#   echo "   ---------------  NEW Inventory Data file found."
for zfile in `ls $DW_DATA_INBOUND_DEPLETION/INVDA*.zip`
do
	 fn_process_inventory_file $zfile Y
done

# Delete old zip and gz files in $DW_DATA_INBOUND_DEPLETION directory
echo "Deleting following files from inbound directory ....."
find $DW_DATA_INBOUND_DEPLETION -name "*done" -mtime +3 -print
find $DW_DATA_INBOUND_DEPLETION -name "VOXREF*" -mtime +3 -print
find $DW_DATA_INBOUND_DEPLETION -name "*done" -mtime +3 -exec rm {} \;
find $DW_DATA_INBOUND_DEPLETION -name "VOXREF*" -mtime +3 -exec rm {} \;

#
# finish batch - SUCCESSFUL
#
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"

exit $DW_RETURN_SUCCESS
