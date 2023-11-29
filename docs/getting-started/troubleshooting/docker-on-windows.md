---
id: docker-on-windows
title: Notes for Windows Users
---

## Free Docker alternative for Windows
You can use Docker Desktop for Windows together with Windows command line or Windows Linux Subsystem (WSL2) for this tutorial. But note that Docker Desktop for Windows needs a license for commercial use
beginning of 2022.

There is a free alternative for Linux or WSL2 called podman from Redhat, which has a compatible command line and also the Dockerfiles are compatible, see [podman.io](https://podman.io/).
Further advantages are that podman is more lightweight - it doesn't need a service and root privileges to run containers.
Install podman on WSL2 Ubuntu:
```
    . /etc/os-release
    echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list
    curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | sudo apt-key add -
    sudo apt-get update
    sudo apt-get -y upgrade
    sudo apt-get -y install podman
```
## Using podman build and podman run

Throughout this tutorial, we often refer to the commands `docker build` and `docker run`.
Podman has identically named commands, which, for the purpose of this tutorial, do exactly the same thing.
So with podman you can just type `podman build` and `podman run` instead.
The podman commands that we provide in our tutorials all assume that they are executed either from a unix environment or from the WSL.

## Equivalent of docker-compose
For [part 2 of this guide](../part-2/delta-lake-format.md), you need docker-compose.
For composing multiple podman containers, you can just execute our custom script podman-compose.sh from the getting-started base directory.

After running the script in the getting-started folder you should now be able to open Polynote on port localhost:8192, as WSL2 automatically publishes all ports on Windows.
If the port is not accessible, you can use `wsl hostname -I` on Windows command line to get the IP adress of WSL, and then access Polynote over \{ip-address}:8192.

## Known Issue with podman on WSL2 on Windows

If you suddenly cannot execute any podman commands anymore and get an error like this:
```
    ERRO[0000] error joining network namespace for container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53
    Error: error joining network namespace of container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53
```
Then you may be experiencing a known problem with podman on WSL2 on windows after a system restart related to the /tmp directory.

If you encounter this error, there are two quick workarounds:
1. Delete the tmp dir of your WSL2 installation and restart WSL2
2. The podman commands with sudo may still work, eg. `sudo podman ps` will work even if `podman ps` wont

See https://github.com/containers/podman/issues/12236 for more information.
