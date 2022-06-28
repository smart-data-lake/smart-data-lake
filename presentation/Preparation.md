# Preparation

Smart Data Lake Builder (SDLB) is designed to be provisioned on a variety of systems, on-prem and in the cloud. For this course we avoid to deal with creating accounts and subsriptions and proper cloud setup and have chosen to run SDLB locally within containers. 
Out target setup will utilize Windows WSL, and podman to run the containers. 
The following guide will assist you with neccessary steps to setup these tools as well as basic setup of SDLB.  

In case you have already a working Unix environment (Linux, MacOS, or Windows Subsystem Linux (WSL) ) you can jump to [Podman installation](#podman). 

If you already have Docker or Podman running, you can directly proceed with [SDLB preparation and testing](#sdlb-preparation-and-testing). 

## Unix environment
Assuming you have no Windows Store available you need to follow the following steps:

* enable WSL: Admin-Powershell -> `dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart`
* enable VM feature: Admin-Powershell -> `dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart`
* get Ubuntu: PowerShell:
	- `Invoke-WebRequest -Uri https://aka.ms/wslubuntu2004 -OutFile Ubuntu.appx -UseBasicParsing` OR
	- `curl.exe -L -o ubuntu-2004.appx https://aka.ms/wslubuntu2004`
* install Ubuntu: Powershell in the directory with the downloaded file
	- `Add-AppxPackage ubuntu-2004.appx`

During the installation we suggest to specify your common user name and a password, preventing to work as root. Your user will have root priviledges (admin) using `sudo`. 
If that worked well you should see something similar to the following when running: `cat /proc/version`:
`Linux version 5.10.16.3-microsoft-standard-WSL2 (oe-user@oe-host) (x86_64-msft-linux-gcc (GCC) 9.3.0`

I you got errors during the installation of Linux try: 

* rename into zip file: PowerShell -> `Rename-Item .\Ubuntu.appx .\Ubuntu.zip`
* extract package: `Expand-Archive .\Ubuntu.zip .\Ubuntu`
* change directory: `cd ./Ubuntu`
* install `Add-AppxPackage  Ubuntu_2004.2021.825.0_x64.appx`

in case of error message of permission issues with the MS store, try

* download and install patch: [https://catalog.s.download.windowsupdate.com/c/msdownload/update/software/updt/2022/05/windows10.0-kb5015020-x64_5a735e4c21ca90801b3e0b019ac210147a978c52.msu](https://catalog.s.download.windowsupdate.com/c/msdownload/update/software/updt/2022/05/windows10.0-kb5015020-x64_5a735e4c21ca90801b3e0b019ac210147a978c52.msu) and try installing Ubuntu again

supporting Documentation:
* [manual install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-manual)
* [manual install WSL actual install](https://docs.microsoft.com/en-us/windows/wsl/install-on-server)
* [MS store patch](https://answers.microsoft.com/en-us/windows/forum/all/microsoft-store-error-code-0xc002001b/8b33966d-6abe-4f17-b04a-bd0744afd9d7)

## Podman
SDLB and other tools will be run in a container. Therefore, we utilize the rootless alternative to Docker, called Podman. The installation is easiest using homebrew:
* install curl and git for downloading sources: `sudo apt update && sudo apt upgrade && sudo apt-get install curl git`
* install homebrew: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`, follow further instructions
* install podman and podman-compose: `brew install podman podman-compose`

## SDLB preparation and testing
* download example case: `git@github.com:smart-data-lake/getting-started.git && git checkout training`
* build SDLB: `podman build -t sdl-spark .`
* try SDLB: `podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel launchTask --help`
