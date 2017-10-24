param(
    $Password,
    $StackName
)
Start-Transcript -Path C:\LabAuCiel\BootAutomation.log

# Turn off password complexity
secedit /export /cfg C:\LabAuCiel\PasswordPolicy.cfg
(Get-Content C:\LabAuCiel\PasswordPolicy.cfg).replace("PasswordComplexity = 1", "PasswordComplexity = 0") | Out-File C:\LabAuCiel\PasswordPolicy.cfg
secedit /configure /db c:\windows\security\local.sdb /cfg C:\LabAuCiel\PasswordPolicy.cfg /areas SECURITYPOLICY

# Create LabAdmin user
NET USER LabAdmin $Password /add /y /expires:never
NET LOCALGROUP Administrators LabAdmin /add

# Prepare Scheduled Task
$command = '-ExecutionPolicy Bypass -Command "C:\LabAuCiel\LabAuCielDeleteStack.ps1 -StackName $($StackName)"'
$command = $ExecutionContext.InvokeCommand.ExpandString($command)
$action = New-ScheduledTaskAction -Execute 'Powershell.exe' -Argument $command
$trigger = New-ScheduledTaskTrigger -Daily -At 12am

# Create Task
$task = Register-ScheduledTask -TaskName "DeleteCFStack" -Action $action -Trigger $trigger -User "LabAdmin" -Password $Password -RunLevel Highest

# Update Task Timing
$task.Triggers.Repetition.Duration = "P1D"
$task.Triggers.Repetition.Interval = "PT1M"
$task | Set-ScheduledTask -User LabAdmin -Password $Password

# Install Chocolatey
Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# Utility Install
choco install googlechrome -y -force
choco install r.project -y -force
choco install r.studio -y -force
choco install notepadplusplus.install -y -force
choco install 7zip.install -y -force
choco install putty.install -y -force
# choco install visualstudiocode -y -force
# choco install winscp -y -force

Stop-Transcript

