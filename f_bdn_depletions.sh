#!/bin/sh
##################################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/f_bdn_depletions.sh,v 1.16 2018/05/02 19:09:34 mfarina Exp $
# $Revision: 1.16 $
# $Name:  $
#
# Shell script : f_bdn_depletions.sh
#
# Description  : This shell script performs following functions to load 
#                data from a staging area into the DW.
#            
#                a) Start batch
#                b) Wait for trigger file (For BDN only)
#                c) Perform following steps on each data file.
#                   1. Validate Data File
#                   2. Load T_table using sqlload.
#                   3. Run the load process to load data from T_table to the DW
#                   4. Archive the data file to archive directory.
#                d) Finish the batch.
#               
# Parameters:  Months - Number of Months to Process
#              System - PRD1 (BDN) or PRD2 (VIP)            
#
# Files: 
#      1. XML --> t_bdn_depletions.xml
#
#      2. CTL --> t_bdn_depletions.ctl
#
#      3. BDN -->BDNDEP*.z
####################################################################################
#set -x

N_MONTHS=$1
if [ -z "$N_MONTHS" ]
then
	N_MONTHS=3
fi

case "$ORACLE_SID" in
     "dwlprd1")  subject=depletion 
             ;;
     "dwlprd2")  subject=reporting 
	     ;;
     *) echo "No database found on server to run this process!!!"
	     exit 10
	     ;;
esac	     


#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------
batch_key=500 # change this

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

if [ "$ORACLE_SID" == "dwlprd1" ]  
then
  #
  # wait for the trigger file to arrive
  #
  dw_wait_for_file -n 720 -d $DW_DATA_INBOUND_BDN/bdndepl.trig
  dw_check_for_error -s "dw_wait_for_file" -e "$?"

  #
  # are there any data files present
  #
  nfiles=`ls $DW_DATA_INBOUND_BDN/bdndepl*.Z | wc -l 2>/dev/null`

  if [ "$nfiles" -gt 0 ]
  then
     #
     #
     # now that the trigger file has arrived, process each data file
     #
     for file in `ls $DW_DATA_INBOUND_BDN/bdndepl*.Z`
     do
        echo "Processing file: $file"
        echo "          Start: `date`"
        echo " "


        echo "---------------  Validate file integrity"
        echo "                    Start: `date`"
  
        $DW_BIN/dw_validate_file_bdn.pl -p "$DW_SUBJECT_CTL/t_bdn_depletions.xml" \
                                    "$file"

        dw_check_for_error -s "dw_validate_file_bdn" \
                        -m "BDN file is invalid"  \
                        -e "$?"
   
        echo "                      End: `date`"
        echo " "

        echo "---------------  sqlload the data file into the T table"
        echo "                    Start: `date`"

        dw_sqlload -c "$DW_SUBJECT_CTL/t_bdn_depletions.ctl" \
                   -r "$DW_RUN_KEY"                           \
                    "$file"
        dw_check_for_error -s "dw_sqlload" -e "$?"
   
        echo "                      End: `date`"
        echo " "


        echo "---------------  sqlplus - run load process"
        echo "                    Start: `date`"

        sql="f_bdn_depletions_pkg.load('"$file"', $N_MONTHS);"

        dw_sqlplus -p "$sql"
        dw_check_for_error -s "dw_sqlplus" -e "$?"

        echo "                      End: `date`"
        echo " "


        echo "---------------  archive the data file"
        echo "                    Start: `date`"

        dw_archive_file -d "$file"
        dw_check_for_error -s "dw_archive_file" -e "$?"

        echo "                      End: `date`"
        echo " "

        echo "            End: `date`"
        echo " "
     done
  #
  # no data files are present, but run process for recycling
  #
  else
     echo "---------------  sqlplus - run load process"
     echo "---------------            no data files, recycling only"
     echo "                    Start: `date`"


     sql="@$DW_SUBJECT_SQL/f_bdn_depletions_load_nofile"

     dw_sqlplus "$sql"
     dw_check_for_error -s "dw_sqlplus" -e "$?"
 
     echo "                      End: `date`"
  fi
else
   RAD_TABLE_OWNER=REPORTING
   RAD_TABLE_NAME=RPT_DD_MONTHLY_RAD
   RAD_PART_TABLE_NAME=RPT_DD_MONTHLY_RAD_PART
   load_failure=0

   export TMPDIR=$DW_DATA_WORK   # TMPDIR need to set to $DW_DATA_WORK because default is /tmp
   outfile=`mktemp -p $DW_DATA_WORK`
   dw_check_for_error -s "mktemp" -m "Make Tempfile Failed" -e "$?"


   echo "---------------  sqlplus - get the months to process"
   echo "                    Start: `date`"

   sql="dd_rad_and_depl_pkg.get_months_to_process;"

   dw_sqlplus -o $outfile -p "$sql"
   dw_check_for_error -s "dw_sqlplus" -e "$?"


   if [ `cat $outfile | grep "^[[:space:]]*#" | sed -e 's/^[[:space:]]*#//' | wc -l` -gt 0 ]
   then
      for line in `cat $outfile | grep "^[[:space:]]*#" | sed -e 's/^[[:space:]]*#//'`
      do
         partition_name=`echo $line | cut -f1 -d:`
         vip_dist_month_date_key=`echo $line | cut -f2 -d:`

         echo "date=" $vip_dist_month_date_key
         echo "part=" $partition_name
         echo "===================================================================="

         echo "---------------  sqlplus - exchange partition (OUT): $partition_name"
         echo "                    Start: `date`"

         sql="dwa_util_pkg.truncate_table('$RAD_PART_TABLE_NAME'); \
              dwa_util_pkg.exchange_partition('$RAD_TABLE_NAME', '$partition_name', '$RAD_PART_TABLE_NAME');"

         dw_sqlplus -p "$sql"
         dw_check_for_error -s "dw_sqlplus" -e "$?"

         echo "                      End: `date`"
         echo " "
         echo " "
         echo " "

         manage_nonunique_ind.ksh $DBNAME $ENV_ID $RAD_TABLE_OWNER $RAD_PART_TABLE_NAME DROP
         dw_check_for_error -s "Manage Index" -m "$error_msg"  \
                            -m "DWC-00001: Index drop failed - $RAD_PART_TABLE_NAME" \
                            -e $?


         echo " "
         echo " "
         echo " "

         echo "---------------  sqlplus - load_month: $vip_dist_month_date_key"
         echo "                    Start: `date`"

         sql="dd_rad_and_depl_pkg.load_rad($vip_dist_month_date_key);"

         dw_sqlplus -o $outfile -p "$sql"
         dw_check_for_error -s "dw_sqlplus" -e "$?"

         echo "                      End: `date`"
         echo " "
         echo " "
         echo " "


         manage_nonunique_ind.ksh $DBNAME $ENV_ID $RAD_TABLE_OWNER $RAD_PART_TABLE_NAME CREATE
         dw_check_for_error -s "Manage Index" -m "$error_msg"  \
                            -m "DWC-00001: Index create failed - $RAD_PART_TABLE_NAME" \
                            -e $?
        echo " "
        echo " "
        echo " "

        analyze_table.ksh $DBNAME $ENV_ID $RAD_TABLE_OWNER $RAD_PART_TABLE_NAME

        echo " "
        echo " "
        echo " "

        echo "---------------  sqlplus - dd_rad_and_depl"
        echo "                    Start: `date`"

        sql="dd_rad_and_depl_pkg.load_depl($vip_dist_month_date_key);"

        dw_sqlplus -o $outfile -p "$sql"
        dw_check_for_error -s "dw_sqlplus" -e "$?"

        echo "                      End: `date`"
        echo " "
        echo " "
        echo " "


        echo "---------------  sqlplus - exchange partition (IN): $partition_name"
        echo "                    Start: `date`"

       sql="dwa_util_pkg.exchange_partition('$RAD_TABLE_NAME', '$partition_name', '$RAD_PART_TABLE_NAME');"

       dw_sqlplus -p "$sql"
       dw_check_for_error -s "dw_sqlplus" -e "$?"

       echo "                      End: `date`"
       echo " "
       echo " "
       echo " "

    done

  else
     echo " "
     echo " ---------------- Nothing to Update  --------------------"
     echo " "
     echo "                      End: `date`"
     echo " "
  fi

  echo "---------------  sqlplus - load rpt_dd_monthly_rad_tx --------------------"
  echo "                    Start: `date`"

  sql="dd_rad_and_depl_pkg.load_rpt_dd_monthly_rad_tx;"

  dw_sqlplus -o $outfile -p "$sql"
  dw_check_for_error -s "dw_sqlplus" -e "$?"

  echo "                      End: `date`"
  echo " "
  echo " "
  echo " "  

  echo "---------------  sqlplus - get data for NEW Outlets"
  echo "                    Start: `date`"

  sql="dd_rad_and_depl_pkg.LOAD_NEW_SEGMENT_BLD;"

  dw_sqlplus -o $outfile -p "$sql"
  dw_check_for_error -s "dw_sqlplus" -e "$?"

  echo "                      End: `date`"
  echo " "  

  rm -f $outfile

  echo "---------------  sqlplus - Ship Equal Deplete Processing"
  echo "                    Start: `date`"

  sql="dd_rad_and_depl_pkg.load_ship_depl;"

  dw_sqlplus -o $outfile -p "$sql"
  dw_check_for_error -s "dw_sqlplus" -e "$?"

  echo "                      End: `date`"
  echo " "  


  subject=forecast
  dw_setup_env $subject
  dw_check_for_error -s "dw_setup_env" -e "$?"

  # move to the work directory
  cd $DW_DATA_WORK

  echo "---------------  sqlplus - run extract_rpt_dd_monthly_rad_bpm.sql "
  echo "                    Start: `date`"

  sname=extract_rpt_dd_monthly_rad_bpm.sql
  fname=Monthly_Retail_Ship_Size_Prem_NRI_`date +%Y%m%d%H%M%S`.csv
 
  outfile=`mktemp -p $DW_DATA_WORK`
 
  if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
  then
      dw_check_for_error -s "mktemp" -m "mktemp failed" -e "$?"
  fi
 
  sql="@$DW_SUBJECT_SQL/extract_rpt_dd_monthly_rad_bpm.sql"
 
  echo "---------------  sqlplus - running script $sname"
  echo "                    Start: `date`"
 
  dw_sqlplus -o $outfile "$sql"
 
  if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
  then
     dw_check_for_error -s "dw_sqlplus" -e "$?"
  fi
 
  echo "                      End: `date`"
  echo " "
 
  # cleanup file here
  echo "---------------  clean - Clean up the output file"
  echo "                    Start: `date`"
  #
  # only get lines that begin with a #, then remove the #
  # make sure any " inside " are ""
  #
  grep '^[[:space:]]*#' $outfile | sed -e 's/^[[:space:]]*#//' -e 's/\([^,]\)"\([^,]\)/\1"\2/' > $DW_DATA_OUTBOUND_FORECAST/$fname
 
  if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
  then
     dw_check_for_error -s "cleanup" -m "Cleaning of file $fname failed" -e "$?"
  fi
 
  echo "                      End: `date`"
  echo " "

fi

#
# finish batch - SUCCESSFUL
#
dw_finish_run_shell -s -r $DW_RUN_KEY $DW_BATCH_KEY
dw_check_for_error -s "dw_finish_run_shell" -e "$?"

exit $DW_RETURN_SUCCESS
