
$connectTestResult = Test-NetConnection -ComputerName storage200or.file.core.windows.net -Port 445
if ($connectTestResult.TcpTestSucceeded) {
    # Save the password so the drive will persist on reboot
    cmd.exe /C "cmdkey /add:`"storage200or.file.core.windows.net`" /user:`"Azure\storage200or`" /pass:`"U8YXkHupPQx8Pl34jCsQFbRxdqEYk9HntiFMf5I1XHv5XHuOkrBqLK9nTXNFTusQonZjeMk9RXXqZheLMV00bA==`""
    # Mount the drive
    New-PSDrive -Name Z -PSProvider FileSystem -Root "\\storage200or.file.core.windows.net\myfiles01" -Persist
} else {
    Write-Error -Message "Unable to reach the Azure storage account via port 445. Check to make sure your organization or ISP is not blocking port 445, or use Azure P2S VPN, Azure S2S VPN, or Express Route to tunnel SMB traffic over a different port."
}