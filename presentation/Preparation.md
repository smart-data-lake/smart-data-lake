# Preparation

Smart Data Lake Builder (SDLB) is designed to be provisioned on a variety of systems, on-prem and in the cloud. For this course we avoid to deal with creating accounts and subscriptions and proper cloud setup and have chosen to run SDLB locally within containers. 
Out target setup will utilize Windows WSL, and podman to run the containers. 
The following guide will assist you with necessary steps to setup these tools as well as basic setup of SDLB.

<!--
For the operating system 2 options are presented: 
* installing [WSL with Ubuntu](#wsl_with_ubuntu) OR
* installing [VirtualBox with Debian](#vitrualbox_with_debian)
Depending on your preferences and your underlying setup, choose one option. 
-->

In case you have already a working Unix environment (Linux, MacOS, or Windows Subsystem Linux (WSL) ) you can jump to [Podman installation](#podman). 

If you already have Docker or Podman running, you can directly proceed with [SDLB preparation and testing](#sdlb-preparation-and-testing). 

## WSL with Ubuntu
Assuming you have no Windows Store available you need to follow the following steps:

* enable WSL: Admin-Powershell -> `dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart`
* enable VM feature: Admin-Powershell -> `dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart`
* set WSL2 as default: `wsl --set-default-version 2`
* get Ubuntu: PowerShell:
	- `Invoke-WebRequest -Uri https://aka.ms/wslubuntu2004 -OutFile Ubuntu.appx -UseBasicParsing` OR
	- `curl.exe -L -o ubuntu-2004.appx https://aka.ms/wslubuntu2004`
* install Ubuntu: Powershell in the directory with the downloaded file
	- `Add-AppxPackage ubuntu-2004.appx`

During the installation we suggest to specify your common user name and a password, preventing to work as root. Your user will have root priviledges (admin) using `sudo`. 
If that worked well you should see something similar to the following when running: `cat /proc/version`:
`Linux version 5.10.16.3-microsoft-standard-WSL2 (oe-user@oe-host) (x86_64-msft-linux-gcc (GCC) 9.3.0`

If you got errors during the installation of Linux try: 

* rename into zip file: PowerShell -> `Rename-Item .\Ubuntu.appx .\Ubuntu.zip`
* extract package: `Expand-Archive .\Ubuntu.zip .\Ubuntu`
* change directory: `cd ./Ubuntu`
* install `Add-AppxPackage  Ubuntu_2004.2021.825.0_x64.appx`

* In case of error message similar to:
	```
	AppxPackage : Deployment failed with HRESULT: 0x80073CF6, Package could not be registered.
	error 0xC002001B: windows.licensing failed to update critical data for
	CanonicalGroupLimited.UbuntuonWindows_2004.2021.825.0_x64__79rhkp1fndgsc.
	```
	download and install patch: [https://catalog.s.download.windowsupdate.com/c/msdownload/update/software/updt/2022/05/windows10.0-kb5015020-x64_5a735e4c21ca90801b3e0b019ac210147a978c52.msu](https://catalog.s.download.windowsupdate.com/c/msdownload/update/software/updt/2022/05/windows10.0-kb5015020-x64_5a735e4c21ca90801b3e0b019ac210147a978c52.msu) and try installing Ubuntu again

supporting Documentation:

* [manual install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-manual)
* [manual install WSL actual install](https://docs.microsoft.com/en-us/windows/wsl/install-on-server)
* [MS store patch](https://answers.microsoft.com/en-us/windows/forum/all/microsoft-store-error-code-0xc002001b/8b33966d-6abe-4f17-b04a-bd0744afd9d7)

If you successfully installed Ubuntu go to step [Podman](#Podman)

<!--
## VirtualBox with Debian
* install [VirtualBox](https://www.virtualbox.org/wiki/Downloads), on Windows download [this](https://download.virtualbox.org/virtualbox/6.1.34/VirtualBox-6.1.34a-150636-Win.exe)
* Download the [Debian image](https://cdimage.debian.org/debian-cd/current/amd64/iso-cd/debian-11.3.0-amd64-netinst.iso)
* after installation open VirtualBox and create a new VM, selecting: Linux, Debian
	- When asked for the bootable disc to install from, select the downloaded iso
	- follow instructions
--> 

## Podman
SDLB and other tools will be run in a container. Therefore, we utilize the rootless alternative to Docker, called Podman:

```
. /etc/os-release
echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list
curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | sudo apt-key add -
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get -y install podman
sudo apt install python3-pip
sudo pip3 install podman-compose==0.1.11
```
> :warning: podman-compose with major 1 (tested up to 1.0.3) do not create pods automatically. Therewith, the used commands results in networking issues between the containers. Thus, we recommend to use the latest version with automatic pod creation, version 0.1.11. The behaviour may change in future versions. 

## SDLB preparation and testing
* download example case: `git clone -b training https://github.com/smart-data-lake/getting-started.git SDLB_training`
* build SDLB: `podman build -t sdl-spark .`
	- this takes ~5-10 min
* podman-compose test: `podman-compose up -d`

* podman-compose: `podman run --rm sdl-spark:latest --config /mnt/config --feed-sel launchTask --help`
	- when you see the SDL help the test was successful.

# Additional hints:
## Git ports
* if in your network the GIT default port 22 is blocked, set in your `~/.ssh/config`:

```BASH
Host github.com
    Hostname ssh.github.com
    IdentityFile ~/.ssh/id_github
    Port 443
    User git
```
