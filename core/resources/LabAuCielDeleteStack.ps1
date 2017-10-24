param(
    $StackName
)

$currentTime = (Get-Date).ToUniversalTime()
$ttlTime = (Get-EC2Tag -Filter @{Name="tag:$($StackName)TTL"; Values="*"}).Value

If ($currentTime -gt $ttlTime) {
    Remove-CFNStack -StackName $($StackName) -Force
} else {
    New-TimeSpan -Start $currentTime -End $ttlTime | Out-File "C:\LabAuCiel\TTLCurrentDifference.log"
}