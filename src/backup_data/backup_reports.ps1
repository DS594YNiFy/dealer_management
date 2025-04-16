$backupDir = "C:\Users\cliankun\user_data\projects\backup\backup_reports\backup_reports_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
New-Item -ItemType Directory -Path $backupDir -Force
Copy-Item -Path "C:\Users\cliankun\user_data\projects\dealer_management\reports" -Destination $backupDir -Recurse -Force
