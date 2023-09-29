<#
.SYNOPSIS
This script copies the provided solutions into your exercises directory.
.DESCRIPTION
USAGE: .\load_solution.ps1 <search string>
<search string>   A string that makes up all, or part, of the exercise name. Exercise numbers (eg. 01) are the simplest form of search.
WARNING:      	RUNNING THIS COMMAND WILL OVERWRITE YOUR CODE. MAKE SURE YOU ACTUALLY WANT TO DO THAT.
#>

param (
	[string]$EXERCISE
)

if (!$EXERCISE) {
	Get-Help $MyInvocation.MyCommand.Definition
	exit
}

$SUB_FOLDERS = @(
    "kwikshoppr/java", 
	"kwikshoppr/sql"
)

$SOLUTION_FOLDER = "../solutions"

$EXERCISE_FOLDER = Get-ChildItem -Path $SOLUTION_FOLDER -Directory -Filter "*$EXERCISE*" | Select-Object -First 1

if (!$EXERCISE_FOLDER) {
	Write-Host "Unable to find a solution for the requested exercise: $EXERCISE"
	Get-Help $MyInvocation.MyCommand.Definition
	exit
}

foreach ($folder in $SUB_FOLDERS) {
	$SOLUTION = Join-Path -Path $EXERCISE_FOLDER.FullName -ChildPath $folder
	$EXERCISE = "./$folder"

	Write-Host "Pulling Solution from $SOLUTION to $EXERCISE"

	if (!(Test-Path -Path $SOLUTION -PathType Container)) {
    	Write-Host "WARNING: Unable to find tests in the requested folder: $SOLUTION...skipping"
    	continue
	}

	if (Test-Path -Path $EXERCISE -PathType Container) {
    	Remove-Item -Path $EXERCISE -Recurse
	}

	New-Item -Path $EXERCISE -ItemType Directory -Force | Out-Null
	Copy-Item -Path $SOLUTION/* -Destination $EXERCISE -Recurse
}
