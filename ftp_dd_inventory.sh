#!/bin/sh
########################################################################
# $Header: /dwa/cvsrepo/dev/depletion/bin/ftp_dd_inventory.sh,v 1.6 2018/12/27 20:45:09 bdingee Exp $
# $Revision: 1.6 $
# $Name:  $
#
# Description  : This script ftp gets the VIP Daily Inventory file for Daily Depletions project
#
# Files: 
#      1. TRIG FILE --> GODDINV.DAT.zip   ????
#      2. DATA FILE --> INVDA.NYYYYMMDD.zip - VIP Daily Inventory file
#                       VOXREF.NYYYYMMDD.gz - VIP product cross reference file (not loaded)
########################################################################
#set -x

#------------------------------------------------
# REQUIRED CODE
#------------------------------------------------

subject=depletion
batch_key=71414

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

this_script=`basename $0`

USER="diauser"
PASSWORD="V5v735VM"
SITE="sftp.vtinfo.com"

load_failure=0
tmpf=`mktemp -p "$DW_DATA_WORK"`  ## File to Delete
tmpf1=`mktemp -p "$DW_DATA_WORK"` ## Zip files on VIP
tmpf2=`mktemp -p "$DW_DATA_WORK"` ## Gzip files on VIP
tmpf3=`mktemp -p "$DW_DATA_WORK"` ## Merged file set

# cd to revenue inbound directory
cd $DW_DATA_INBOUND_DEPLETION
#
# loop checking for the trigger file until it appears
# or 1 hour (15sec * 240) has elapsed
#
n=0
trigger="N"
file_count=0

TRIGGER_FILE=GODDINV.DAT.zip

function fn_get_trigger ()
{
  sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE <<EOF
    get $TRIGGER_FILE
    bye
EOF
}

function fn_remove_trigger ()
{
  sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE <<EOF
    rm $TRIGGER_FILE
    bye
EOF
}

function fn_get_files_to_delete ()
{
 sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE >$tmpf1 <<EOF
    ls -l dd-*zip.dgo
    bye
EOF
}

function fn_delete_files ()
{
 sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE <<EOF
    rm $fname
    bye
EOF
}

function fn_list_files ()
{
 sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE >$tmpf2 <<EOF
    ls -l *.zip 
    bye
EOF
}

function fn_get_files ()
{
 sftp -oIdentityFile=/home/dwaadm/.ssh/id_rsa -o"ProxyCommand /usr/bin/nc --proxy gateway.zscaler.net:10085 %h %p" -oPort=22 $USER@$SITE <<EOF
    get $fname
	rename $fname dd-${fname}.dgo
    bye
EOF
}

copy_to_opswise ()
{
 export PATH=/opt/rh/rh-dotnetcore11/root/usr/bin${PATH:+:${PATH}}
 export LD_LIBRARY_PATH=/opt/rh/rh-dotnetcore11/root/usr/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
 export MANPATH=/opt/rh/rh-dotnetcore11/root/usr/share/man${MANPATH:+:${MANPATH}}
 export PKG_CONFIG_PATH=/opt/rh/rh-dotnetcore11/root/usr/lib64/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}
 export PYTHONPATH=/opt/rh/rh-dotnetcore11/root/usr/lib/python2.7/site-packages${PYTHONPATH:+:${PYTHONPATH}}
 # dotnet reports metrics to some remote server by default. disable that.

 export DOTNET_CLI_TELEMETRY_OPTOUT=true

 azcopy --source /dwa/data/prd/inbound/depletion --destination https://stadiageodlopswisetst01.file.core.windows.net/datalake/Inbound/BPM/VIP --dest-key jUOqjO25VhMlAXsZNwwcsdlx6FoEDQMQWy5uOPzYOvhoUFCFfhVmEWpYvyiiypl51dQoXaH9cwp2dsz8ani+3Q== --include "INVDA*" --resume /home/dwaadm/Microsoft/Azure/AzCopy/VIP_INVENTORY --quiet

 azcopy --source /dwa/data/prd/inbound/depletion --destination https://stadiageodlopswiseprd01.file.core.windows.net/datalake/Inbound/BPM/VIP --dest-key cA6pel5NaHvWaCuoUuwy4HSZ+6yq97+ouFP+8wsJPiLJR8bXSdxwENSud4TFY+ssRMMgUB5w/RuCwBUlBTVlrQ== --include "INVDA*" --resume /home/dwaadm/Microsoft/Azure/AzCopy/VIP_INVENTORY --quiet
}

echo "Waiting for trigger file"

while [ $n -le 35 ] && [ "$trigger" != "Y" ]
do
   fn_get_trigger   

   if [ -a $TRIGGER_FILE ]
   then
      trigger="Y"
	  fn_remove_trigger
   else
      n=`expr $n + 1`
      echo "going to sleep"
      sleep 60
   fi
done

if [ "$trigger" = "N" ]
then
   # don't treat no trigger file as a failure
   echo "ftp_dd_inventory.sh: No Trigger File: $TRIGGER_FILE" 
   echo "No VIP trigger file GODDINV.DAT.zip found --- data not received today" | mailx -s "FTP_DD_INVENTORY Alert" dw_support swistrand
   load_failure=0
else
   echo "Getting list of files to delete: *.dgo"
   tmpf1=`mktemp -p "$DW_DATA_WORK"`  ## File to Delete
   tmpf2=`mktemp -p "$DW_DATA_WORK"`  ## List of new files	 
	 
   # delete prior days file  
   fn_get_files_to_delete

   if [ "$?" -ne 0 ]
   then
      # error ftp'ing
      error_msg="DWC-00001: getting list of file names failed"
      echo "$error_msg"
      dw_create_error_shell -s "Get List of Files" -m "$error_msg"
   fi
   
   cat $tmpf1 | grep dgo | awk -F" " '{print $NF}'
	 
   echo "Deleting files"
   echo ""
   for fname in `cat $tmpf1 | grep dgo | awk -F" " '{print $NF}' | grep -E '(INVDA|VOXREF)'` 
   do	 
      fn_delete_files
	
      if [ "$?" -ne 0 ]
      then
         # error ftp'ing
         error_msg="DWC-00001: remove $fname failed"
         echo "$error_msg"
         dw_create_error_shell -s "Remove Data File: $fname " -m "$error_msg"
         load_failure=1
         break
      fi
   done
	  
   # get list of new files 
   echo "Getting list of available data files: *.gz"
   fn_list_files
 
   if [ "$?" -ne 0 ]
   then
      # error ftp'ing
      error_msg="DWC-00001: getting list of file names failed"
      echo "$error_msg"
      dw_create_error_shell -s "Get List of Files" -m "$error_msg"
   fi
 
   # get new files 
   cat $tmpf2 | grep zip | awk -F" " '{print $NF}' 
   for fname in `cat $tmpf2 | grep zip | awk -F" " '{print $NF}' | grep -E '^(INVDA|VOXREF)'` 
   do
      fn_get_files

      if [ "$?" -ne 0 ]
      then
         # error ftp'ing
         error_msg="DWC-00001: get $file failed"
         echo "$error_msg"
         dw_create_error_shell -s "Get Data File: $file" -m "$error_msg"
         load_failure=1
         break
      fi
   done
      
   rm -f $tmpf1
   rm -f $tmpf2
   copy_to_opswise	 
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
