# Create a new admin user and allow him to use elevated priveleges
New-LocalUser -Name circleci -Password $(ConvertTo-SecureString "INSTANCE_PASSWORD" -AsPlainText -Force)
Add-LocalGroupMember -Group "Administrators" -Member "circleci"
# https://github.com/PowerShell/Win32-OpenSSH/issues/962
reg add HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\system /v LocalAccountTokenFilterPolicy /t REG_DWORD /d 1 /f
