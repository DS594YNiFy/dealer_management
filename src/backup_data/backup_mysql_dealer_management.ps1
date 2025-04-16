$mysqlUser = "root"
$mysqlPassword = "root"
$mysqlHost = "localhost"
$databaseName = "distributor_sales_db"

$backupFilePath = "C:\Users\cliankun\user_data\projects\backup\backup_mysql\dealer_management_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql"

$backupDirectory = Split-Path -Path $backupFilePath -Parent
if (-not (Test-Path -Path $backupDirectory)) {
    New-Item -ItemType Directory -Path $backupDirectory | Out-Null
}

$mysqldumpPath = "C:\program_files\Mysql\bin\mysqldump"
$mysqldumpCommand = "$mysqldumpPath -u $mysqlUser -p$mysqlPassword -h $mysqlHost $databaseName > $backupFilePath"

try {
    Invoke-Expression -Command $mysqldumpCommand
    Write-Host "数据库备份成功，备份文件路径：$backupFilePath"
}
catch {
    Write-Host "数据库备份失败：$($_.Exception.Message)"
}
