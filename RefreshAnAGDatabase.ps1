# Refresh AMTREFRESHTEST
Param([String]$DatabaseName = "AMTREFRESHTEST")

# Create SQL SMO object so that we can use sqlcmd)
Import-Module "sqlps" -DisableNameChecking


$Retention = "24h"

# Servers
$SQLProdListener = "ProdListenerName" 
$SQLPreProdListener = "PreProdListenerName"

$SQLProdPrimary = (Invoke-Sqlcmd -ServerInstance $SQLProdListener -Query "select cs.replica_server_name,cn.node_name
from sys.dm_hadr_availability_replica_states as n 
    join sys.dm_hadr_availability_replica_cluster_states as cs on cs.replica_id = n.replica_id 
	join sys.dm_hadr_availability_replica_cluster_nodes as cn on cn.replica_server_name = cs.replica_server_name
	where n.role_desc = 'primary'")
$ProdPrimary = $SQLProdPrimary.node_name

$SqlPreProdPrimary = (Invoke-Sqlcmd -ServerInstance $SQLPreProdListener -Query "select cs.replica_server_name,cn.node_name
from sys.dm_hadr_availability_replica_states as n 
    join sys.dm_hadr_availability_replica_cluster_states as cs on cs.replica_id = n.replica_id 
	join sys.dm_hadr_availability_replica_cluster_nodes as cn on cn.replica_server_name = cs.replica_server_name
	where n.role_desc = 'primary'")
$PreProdPrim = $SqlPreProdPrimary.node_name

$SqlPreProdSecondary = (Invoke-Sqlcmd -ServerInstance $SQLPreProdListener -Query "select cs.replica_server_name,cn.node_name
from sys.dm_hadr_availability_replica_states as n 
    join sys.dm_hadr_availability_replica_cluster_states as cs on cs.replica_id = n.replica_id 
	join sys.dm_hadr_availability_replica_cluster_nodes as cn on cn.replica_server_name = cs.replica_server_name
	where n.role_desc = 'secondary'")
$PreProdSec = $SqlPreProdSecondary.node_name

$AGName = "AvailabilityGroupName"

$SQlSvr1 = New-Object Microsoft.SqlServer.Management.Smo.Server $SQLPreProdListener

# Paths
$SourcePath = "\\$ProdPrimary\h$\Backup\BackupsForPPRefreshes\"  # backup path on prod
$DestinationPath = "\\$PreProdPrim\t$\Backup\BackupsForPPRefreshes\"  # backup path on pre prod
$ExportPath = "\\$PreProdPrim\h$\Backup\BackupsForPPRefreshes\" # path to log files on pre prod  
$AGBackupPath= "\\$PreProdPrim\h$\Backup\ForAG\" # backup path on pre prod for copy only backups and restores for AG
$AGSecondaryPath = "\\$PreProdSec\h$\Backup\ForAG\"  # backup path on pre prod secondary node for AG backups

# Filenames
$CurrentBackup_FName = "$DatabaseName.bak"
$NewBackup_FName = "$DatabaseName-$(get-date -f yyyy-MM-dd-hh-mm-ss).bak"
$CurrentDatabase_FName = "$DatabaseName.bak"
$AGDatabase_FName = ($DatabaseName +"_CopyOnly.bak")
$AGDatabase_LogFName = ($DatabaseName +"_CopyOnly.trn")
$LogFileDate = Get-Date -Format FileDateTime
$Refreshlogfile = $ExportPath + $DatabaseName + "_RefreshLog_" + $LogFileDate +".log"
$PreRefresh_FName = ($DatabaseName + "_PreRefresh_-$(get-date -f yyyy-MM-dd-hh-mm-ss).bak")
$UserFilePath = $ExportPath + $DatabaseName + "_Users.sql"

 Write-Verbose -Message  "********** THE DATABASE BEING REFRESHED IS $DatabaseName **********" -verbose 4> $Refreshlogfile 

#Check if log file is past retention and if it is delete it
Get-ChildItem $ExportPath -Recurse -File  -Include *.log| Where CreationTime -lt  (Get-Date).AddHours(-24) | Remove-Item -Force -Verbose 4>> $Refreshlogfile


# BACKUP PROD DATABASE AND COPY TO PRE PROD 

# Purge prod backups past retention from source
Remove-DbaBackup -RetentionPeriod $Retention -BackupFileExtension bak -Path $SourcePath -Verbose 4>> $Refreshlogfile


#Check if file exists in source and if it does rename it
Switch (Test-Path $SourcePath$CurrentBackup_FName)
{
  
   True {Rename-Item -Path $SourcePath$CurrentBackup_FName  -NewName $SourcePath$NewBackup_FName  
   Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) The backup file has been renamed to $SourcePath$NewBackup_FName" |Out-file $Refreshlogfile -append}
   False {Write-Warning "$SourcePath$CurrentBackup_FName does not exist"}
}


# Take a copy only backup of the prod database
Backup-SqlDatabase -ServerInstance $SQLProdListener -Database $DatabaseName -BackupFile $SourcePath$CurrentBackup_FName -CopyOnly -Checksum `  
Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) Copy only backup of $DatabaseName on $DatabaseName has been completed" |Out-file $Refreshlogfile -append

# Purge pre prod backups past retention from destination
Remove-DbaBackup -RetentionPeriod $Retention -BackupFileExtension bak -Path $DestinationPath -Verbose 4>> $Refreshlogfile

#Check if file exists in destination and if it does rename it
Switch (Test-Path $DestinationPath$CurrentBackup_FName)
{
   True {Rename-Item -Path $DestinationPath$CurrentBackup_FName  -NewName $DestinationPath$NewBackup_FName -Verbose 4>> $Refreshlogfile}
   False {Write-Warning "$DestinationPath$CurrentBackup_FName does not exist"}
}
 

# Copy backup files from prod to pre prod
Switch (Test-Path $SourcePath$CurrentBackup_FName)
{
    True {Copy-Item $SourcePath$CurrentBackup_FName -Destination $DestinationPath -Verbose 4>> $Refreshlogfile}
    False {Write-Warning "$SourcePath$CurrentBackup_FName does not exist"}
}
# Take a Pre Prod Backup Prior to Refresh
Backup-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $DestinationPath\$PreRefresh_FName  -Checksum -BackupAction Database  -Verbose 4>> $Refreshlogfile 


# RESTORE INTO PRE PROD

# Get a list of database roles and members
Export-DbaUser -SqlInstance $SQLPreProdListener  -Database $DatabaseName -filePath $UserFilePath 

# remove from availability group
Remove-DbaAgDatabase -SqlInstance $SQLPreProdListener -AvailabilityGroup $AGName -Database $DatabaseName -Confirm:$false  -Verbose 4>> $Refreshlogfile

# check the backup files exists then restore over the existing database in pre prod
Switch (Test-Path $DestinationPath\$CurrentBackup_FName)
{
    True{
        $SQlSvr1.KillAllprocesses($DatabaseName)   
        Set-DbaDbState -SqlInstance $SQLPreProdListener -Database $DatabaseName -SingleUser -Force  -Verbose 4>> $Refreshlogfile
        Restore-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $DestinationPath\$CurrentBackup_FName -ReplaceDatabase -Verbose 4>> $Refreshlogfile
        Set-DbaDbState -SqlInstance $SQLPreProdListener -Database $DatabaseName -MultiUser -Force -Verbose 4>> $Refreshlogfile   
        }
        False {Write-Warning "$DestinationPath$CurrentBackup_FName does not exist"}
}

# add role members to restored database
Invoke-Sqlcmd -ServerInstance $SQLPreProdListener -Database $DatabaseName -InputFile $UserFilePath -Verbose 4>> $Refreshlogfile


# Stop the log backups
Invoke-Sqlcmd -ServerInstance $SQLPreProdListener -Database MSDB -Query "EXEC msdb.dbo.sp_update_job @job_name = 'OlaMaintenance_Backup_Log', @enabled=0" -Verbose 4>> $Refreshlogfile
Invoke-Sqlcmd -ServerInstance $PreProdSec -Database  MSDB -Query "EXEC msdb.dbo.sp_update_job @job_name = 'OlaMaintenance_Backup_Log', @enabled=0" -Verbose 4>> $Refreshlogfile

# If FULL backup file exists delete it before taking a backup
Switch (Test-Path $AGBackupPath\$AGDatabase_FName)
{
    True{
        Remove-Item $AGBackupPath\$AGDatabase_FName -Verbose 4>> $Refreshlogfile
        Backup-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_FName  -Checksum -BackupAction Database  -Verbose 4>> $Refreshlogfile 
        }
        False {Backup-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_FName  -Checksum -BackupAction Database  -Verbose 4>> $Refreshlogfile}
}


# If LOG backup file exists delete it before taking a log backup
Switch (Test-Path $AGBackupPath\$AGDatabase_LogFName)
{
    True{
        Remove-Item $AGBackupPath\$AGDatabase_LogFName
        Backup-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_LogFName  -Checksum -BackupAction Log  -Verbose 4>> $Refreshlogfile
        }
        False {Backup-SqlDatabase -ServerInstance $SQLPreProdListener -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_LogFName  -Checksum -BackupAction Log  -Verbose 4>> $Refreshlogfile}
}

# Copy AG backups to secondary node
Switch (Test-Path $AGBackupPath\$AGDatabase_FName)
{
    True {Copy-Item $AGBackupPath\$AGDatabase_FName -Destination $AGSecondaryPath\$AGDatabase_FName  -Verbose 4>> $Refreshlogfile}
    False {Write-Warning "$AGBackupPath\$AGDatabase_FName does not exist"}
}
Switch (Test-Path $AGBackupPath\$AGDatabase_LogFName)
{
    True {Copy-Item $AGBackupPath\$AGDatabase_LogFName -Destination $AGSecondaryPath\$AGDatabase_LogFName  -Verbose 4>> $Refreshlogfile}
    False {Write-Warning "$AGBackupPath\$AGDatabase_LogFName does not exist"}
}

# Drop database from secondary
$PSDefaultParameterValues['*:Confirm'] = $false # stop the remove command from prompting to confirm
Remove-DbaDatabase -sqlinstance $PreProdSec -Database $DatabaseName -Verbose 4>> $Refreshlogfile 


# Restore full backup no recovery
Switch (Test-Path $AGBackupPath\$AGDatabase_FName)
{
    True{Restore-SqlDatabase -ServerInstance $PreProdSec -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_FName -NoRecovery -Verbose 4>> $Refreshlogfile}
    False {Write-Warning "$AGBackupPath\$AGDatabase_FName does not exist"}
}

# Restore log backup no recovery
Switch (Test-Path $AGBackupPath\$AGDatabase_LogFName)
{
    True{Restore-SqlDatabase -ServerInstance $PreProdSec -Database $DatabaseName -BackupFile $AGBackupPath\$AGDatabase_LogFName -NoRecovery -Verbose 4>> $Refreshlogfile}
    False {Write-Warning "$AGBackupPath\$AGDatabase_LogFName does not exist"}
}

# ADD DATABASE BACK INTO AVAILABILITY GROUP

try
{
    # clear any errors so we can capture an accurate error count from this block
    $error.Clear() ; "errors cleared" 
    $AGErr = $error.Count

    # Add database into AG on primary
    $SqlStringPrimary = "sqlserver:\sql\$PreProdPrim\default\availabilitygroups\$AGName" 
    Add-SqlAvailabilityDatabase -Path $sqlstringprimary -Database $DatabaseName 

    # Add database into AG on secondary
    $SqlStringSecondary = "sqlserver:\sql\$PreProdSec\default\availabilitygroups\$AGName"
    Add-SqlAvailabilityDatabase -Path $SqlStringSecondary -Database $DatabaseName 
    
    # capture the error count so we can do something if a non-terminating error occurs
    $AGErr = $error.Count
}
catch
{
#check if database is already in the availability group and if it is leave it there and carry on to the section to enable the backups
    $AGCheck = Get-DbaAgDatabase -SqlInstance $PreProdPrim -AvailabilityGroup $AGName -Database $DatabaseName

    If($AGCheck.Name -eq $DatabaseName )
    {"$DatabaseName in AG" *>> $Refreshlogfile}
    Else
    {
     "An error occurred adding the database to the availability group" *>> $Refreshlogfile
   
    # Remove the database from the availability group on the primary so that the availability group is not broken for the other databases
    Remove-DbaAgDatabase -SqlInstance $SQLPreProdListener -AvailabilityGroup $AGName -Database $DatabaseName -Confirm:$false -Verbose 4>> $Refreshlogfile
    

    # Drop database from secondary
    $PSDefaultParameterValues['*:Confirm'] = $false # stop the remove command from prompting to confirm
    Remove-DbaDatabase -sqlinstance $PreProdSec -Database $DatabaseName -Verbose 4>> $Refreshlogfile 
    }
 
}

<# 
    If something went wrong adding the database to the AG, that did not produce a terminating error, this will remove it to make sure the AG is not broken for the other database should a failover occur.
    Then the copy on the secondary will be dropped.

#>

If ($AGErr -ne 0) {
  Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) An error occurred adding the database to the availability group" |Out-file $Refreshlogfile -append
  Remove-DbaAgDatabase -SqlInstance $SQLPreProdListener -AvailabilityGroup $AGName -Database $DatabaseName -Confirm:$false -Verbose 4>> $Refreshlogfile
  Remove-DbaDatabase -sqlinstance $PreProdSec -Database $DatabaseName -Verbose 4>> $Refreshlogfile 
  }  
Else 
{

    Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) Database successfully joined to availability group" |Out-file $Refreshlogfile -append
} 


# Enable log backup jobs
$error.Clear() ; "errors cleared" 
$AgentErr = $error.Count

Invoke-Sqlcmd -ServerInstance $PreProdPrim -Database MSDB -Query "EXEC msdb.dbo.sp_update_job @job_name = 'OlaMaintenance_Backup_Log', @enabled=1" -Verbose 4>> $Refreshlogfile
 
if ($AgentErr -ne 0) {
   Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) An error occurred enabling the $PreProdPrim transaction log backup job, check if the job is enabled" |Out-file $Refreshlogfile -append
   $error.Clear() ; "errors cleared" 
   $AgentErr = $error.Count
   }
else
{
  Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) The $PreProdPrim transaction log backup job has been enabled" |Out-file $Refreshlogfile -append
 
}


Invoke-Sqlcmd -ServerInstance $PreProdSec -Database MSDB -Query "EXEC msdb.dbo.sp_update_job @job_name = 'OlaMaintenance_Backup_Log', @enabled=1" -Verbose 4>> $Refreshlogfile

if ($AgentErr -ne 0) {
   Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) An error occurred enabling the the $preprodSec transaction log backup job, check if the job is enabled" |Out-file $Refreshlogfile -append
   $error.Clear() ; "errors cleared" 
   $AgentErr = $error.Count
   }
else
{
  Write-Output "$('[{0:dd/MM/yyyy} {0:HH:mm:ss}]' -f (Get-Date)) The $preprodSec transaction log backup job has been enabled" |Out-file $Refreshlogfile -append
  
}