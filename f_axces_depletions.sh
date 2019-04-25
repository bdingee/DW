#!/bin/sh
########################################################################
# $Header : $
# $REVISION: $
# $Name : $
#
# Shell script : f_axces_depletions.sh
#
# Description  : This script summarizes data from F_VIP_ACCTSOLD into F_AXCES_DEPLETION
#
########################################################################
#set -x

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion 
batch_key=506 

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


echo "      ---------------  sqlplus - run load process"
echo "                       Start: `date`"
sql="f_axces_depletions_pkg.load;"
dw_sqlplus -p "$sql"
dw_check_for_error -s "dw_sqlplus" -e "$?"
echo "                         End: `date`"


#
# finish batch - SUCCESSFUL
#
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"


exit $DW_RETURN_SUCCESS

