<#
.SYNOPSIS
This script copies the provided setup files into your exercises directory.
.DESCRIPTION
USAGE: .\load_exercise.ps1 <search string>
<search string>   A string that makes up all, or part, of the exercise name. Exercise numbers (eg. 01) are the simplest form of search.
WARNING:      	RUNNING THIS COMMAND WILL OVERWRITE YOUR SQL FILES. YOU WILL LOSE ANY CHANGES YOU HAVE MADE CHANGES TO THE FILES.
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

$SETUP_FOLDER = "../setup"

$EXERCISE_FOLDER = Get-ChildItem -Path $SETUP_FOLDER -Directory -Filter "*$EXERCISE*" | Select-Object -First 1

if (!$EXERCISE_FOLDER) {
	Write-Host "Unable to find a setup folder for the requested exercise: $EXERCISE"
	Get-Help $MyInvocation.MyCommand.Definition
	exit
}

foreach ($folder in $SUB_FOLDERS) {
	$SETUP = Join-Path -Path $EXERCISE_FOLDER.FullName -ChildPath $folder
	$EXERCISE = "./$folder"

	Write-Host "Loading setup files from $SETUP to $EXERCISE"

	if (!(Test-Path -Path $SETUP -PathType Container)) {
    	Write-Host "WARNING: Unable to find setup files in the requested folder: $SETUP...skipping"
    	continue
	}

	if (Test-Path -Path $EXERCISE -PathType Container) {
    	Remove-Item -Path $EXERCISE -Recurse
	}

	New-Item -Path $EXERCISE -ItemType Directory -Force | Out-Null
	Copy-Item -Path $SETUP/* -Destination $EXERCISE -Recurse
}
