	#!/bin/sh
	###############################################################################
	# $Header: /dwa/cvsrepo/dev/depletion/bin/bw_depletion_extract.sh,v 1.16 2019/03/18 19:32:38 bdingee Exp $
	# $Revision: 1.16 $
	# $Name:  $
	#
	# Description  : create depl & Inventory  extract file for NA01, NA07, NA02 for BW
	#
	# MODIFICATION HISTORY
	# Person      Date     Comments
	# ---------   -------- ----------------------------------------
	#
	################################################################################
	#set -x

	#------------------------------------------------
	# REQUIRED CODE
	#------------------------------------------------

	subject=depletion
	batch_key=516

	USER=bpmdsp1
	PASSWORD=bpmdsp199
	SITE=10.114.55.103
	SUBDIR=Depletion

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

	echo "      ---------------  sqlplus - load extract tables "
	echo "                       Start: `date`"
	sql="bw_depletions_extract_pkg.load;"

	dw_sqlplus -p "$sql"
	dw_check_for_error -s "dw_sqlplus" -e "$?"
	echo "                         End: `date`"
	echo " "

	for file in depletion inventory 
	do
	  case $file in depletion) cleanfile="DPDCDNAM`date +%Y%m%d%H%M%S`"
								;;
					inventory) cleanfile="DPDCINAM`date +%Y%m%d%H%M%S`"
								;;
							*) continue
								;;
	  esac
	  
	  sql="@$DW_SUBJECT_SQL/bw_${file}_extract_daily.sql"  
	  outfile=`mktemp -p $DW_DATA_WORK`
	  if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
	  then
		 dw_check_for_error -s "mktemp" -m "mktemp failed" -e "$?"
	  fi  
	  
	  echo "---------------  sqlplus - run extract process for $file "
	  echo "                    Start: `date`"
	  dw_sqlplus -o $outfile "$sql"

	  if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
	  then
		 dw_check_for_error -s "dw_sqlplus" -e "$?"
	  fi

	  echo "                      End: `date`"
	  echo " "

	  # cleanup file here
	  echo "---------------  clean - Clean up output file"
	  echo "                    Start: `date`"

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
	 
	  #FTP the file to BW PRD only if we are running it from DW PRD Server AND SendFTP flag=Y
	  machine=`hostname`

	  if [ "$machine" = "nwkdwprd1" -o "$machine" = "nwkdwprd4" ] 
	  then
		 echo " "
		 echo "---------------  FTP the data file"
		 dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -m ASCII -c put "$cleanfile.csv"

		 if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
		 then
			dw_check_for_error -s "FTP" -m "FTP Error" -e "$?"
		 fi
	  else
		 echo " "
		 echo "---------------  DID NOT FTP as $machine is not the PRD Server"
	  fi
	  
	  echo "                      End: `date`"
	  echo " "
	   
	  # cleanup
	  gzip $cleanfile.csv
	  mv  $cleanfile.csv.gz $DW_DATA_OUTBOUND_DEPLETION
	done

	#Put the trigger file.
	trigFile=Depletion.trig
	touch $trigFile

	if [ "$machine" = "nwkdwprd1" -o "$machine" = "nwkdwprd4" ]
	then
	   echo " "
	   echo "---------------  FTP the trigger file"
	   dw_ftp -u $USER -p $PASSWORD -d $SITE -s $SUBDIR -m ASCII -c put "$trigFile"
	   if [ "$?" -ne "$DW_RETURN_SUCCESS" ]
	   then
		  dw_check_for_error -s "FTP" -m "FTP Error Trigger File" -e "$?"
	   fi
	fi
	echo "                      End: `date`"
	echo " "

	echo "      ---------------  sqlplus - update date table"
	echo "                       Start: `date`"
	sql="bw_depletions_extract_pkg.update_run_table;"

	dw_sqlplus -p "$sql"
	dw_check_for_error -s "dw_sqlplus" -e "$?"
	echo "                         End: `date`"
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

