
$folderPath = "C:\Users\CYLK_\user_data\records\archived\20241021_zippo\meeting"
$subfolders = Get-ChildItem -Path $folderPath -Directory
$files = Get-ChildItem -Path $folderPath -File

foreach ($subfolder in $subfolders) {
    Write-Output $subfolder.Name
}
foreach ($file in $files) {
    Write-Output $file.Name
}


