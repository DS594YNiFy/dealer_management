$backupDir = "C:\Users\cliankun\user_data\projects\backup\backup_dealer_management\backup_dealer_management_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
New-Item -ItemType Directory -Path $backupDir -Force
Copy-Item -Path "C:\Users\cliankun\user_data\projects\dealer_management" -Destination $backupDir -Recurse -Force
