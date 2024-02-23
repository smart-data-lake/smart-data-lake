"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[4200],{3026:(e,n,o)=>{o.r(n),o.d(n,{assets:()=>d,contentTitle:()=>i,default:()=>u,frontMatter:()=>s,metadata:()=>a,toc:()=>c});var t=o(4848),r=o(8453);const s={id:"docker-on-windows",title:"Notes for Windows Users"},i=void 0,a={id:"getting-started/troubleshooting/docker-on-windows",title:"Notes for Windows Users",description:"Free Docker alternative for Windows",source:"@site/docs/getting-started/troubleshooting/docker-on-windows.md",sourceDirName:"getting-started/troubleshooting",slug:"/getting-started/troubleshooting/docker-on-windows",permalink:"/docs/getting-started/troubleshooting/docker-on-windows",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/troubleshooting/docker-on-windows.md",tags:[],version:"current",frontMatter:{id:"docker-on-windows",title:"Notes for Windows Users"},sidebar:"tutorialSidebar",previous:{title:"Common Problems",permalink:"/docs/getting-started/troubleshooting/common-problems"},next:{title:"Build SDL",permalink:"/docs/reference/build"}},d={},c=[{value:"Free Docker alternative for Windows",id:"free-docker-alternative-for-windows",level:2},{value:"Using podman build and podman run",id:"using-podman-build-and-podman-run",level:2},{value:"Equivalent of docker-compose",id:"equivalent-of-docker-compose",level:2},{value:"Known Issue with podman on WSL2 on Windows",id:"known-issue-with-podman-on-wsl2-on-windows",level:2}];function l(e){const n={a:"a",code:"code",h2:"h2",li:"li",ol:"ol",p:"p",pre:"pre",...(0,r.R)(),...e.components};return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsx)(n.h2,{id:"free-docker-alternative-for-windows",children:"Free Docker alternative for Windows"}),"\n",(0,t.jsx)(n.p,{children:"You can use Docker Desktop for Windows together with Windows command line or Windows Linux Subsystem (WSL2) for this tutorial. But note that Docker Desktop for Windows needs a license for commercial use\nbeginning of 2022."}),"\n",(0,t.jsxs)(n.p,{children:["There is a free alternative for Linux or WSL2 called podman from Redhat, which has a compatible command line and also the Dockerfiles are compatible, see ",(0,t.jsx)(n.a,{href:"https://podman.io/",children:"podman.io"}),".\nFurther advantages are that podman is more lightweight - it doesn't need a service and root privileges to run containers.\nInstall podman on WSL2 Ubuntu:"]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{children:'    . /etc/os-release\n    echo "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list\n    curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | sudo apt-key add -\n    sudo apt-get update\n    sudo apt-get -y upgrade\n    sudo apt-get -y install podman\n'})}),"\n",(0,t.jsx)(n.h2,{id:"using-podman-build-and-podman-run",children:"Using podman build and podman run"}),"\n",(0,t.jsxs)(n.p,{children:["Throughout this tutorial, we often refer to the commands ",(0,t.jsx)(n.code,{children:"docker build"})," and ",(0,t.jsx)(n.code,{children:"docker run"}),".\nPodman has identically named commands, which, for the purpose of this tutorial, do exactly the same thing.\nSo with podman you can just type ",(0,t.jsx)(n.code,{children:"podman build"})," and ",(0,t.jsx)(n.code,{children:"podman run"})," instead.\nThe podman commands that we provide in our tutorials all assume that they are executed either from a unix environment or from the WSL."]}),"\n",(0,t.jsx)(n.h2,{id:"equivalent-of-docker-compose",children:"Equivalent of docker-compose"}),"\n",(0,t.jsxs)(n.p,{children:["For ",(0,t.jsx)(n.a,{href:"/docs/getting-started/part-2/delta-lake-format",children:"part 2 of this guide"}),", you need docker-compose.\nFor composing multiple podman containers, you can just execute our custom script podman-compose.sh from the getting-started base directory."]}),"\n",(0,t.jsxs)(n.p,{children:["After running the script in the getting-started folder you should now be able to open Polynote on port localhost:8192, as WSL2 automatically publishes all ports on Windows.\nIf the port is not accessible, you can use ",(0,t.jsx)(n.code,{children:"wsl hostname -I"})," on Windows command line to get the IP adress of WSL, and then access Polynote over {ip-address}:8192."]}),"\n",(0,t.jsx)(n.h2,{id:"known-issue-with-podman-on-wsl2-on-windows",children:"Known Issue with podman on WSL2 on Windows"}),"\n",(0,t.jsx)(n.p,{children:"If you suddenly cannot execute any podman commands anymore and get an error like this:"}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{children:'    ERRO[0000] error joining network namespace for container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53\n    Error: error joining network namespace of container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53\n'})}),"\n",(0,t.jsx)(n.p,{children:"Then you may be experiencing a known problem with podman on WSL2 on windows after a system restart related to the /tmp directory."}),"\n",(0,t.jsx)(n.p,{children:"If you encounter this error, there are two quick workarounds:"}),"\n",(0,t.jsxs)(n.ol,{children:["\n",(0,t.jsx)(n.li,{children:"Delete the tmp dir of your WSL2 installation and restart WSL2"}),"\n",(0,t.jsxs)(n.li,{children:["The podman commands with sudo may still work, eg. ",(0,t.jsx)(n.code,{children:"sudo podman ps"})," will work even if ",(0,t.jsx)(n.code,{children:"podman ps"})," wont"]}),"\n"]}),"\n",(0,t.jsxs)(n.p,{children:["See ",(0,t.jsx)(n.a,{href:"https://github.com/containers/podman/issues/12236",children:"https://github.com/containers/podman/issues/12236"})," for more information."]})]})}function u(e={}){const{wrapper:n}={...(0,r.R)(),...e.components};return n?(0,t.jsx)(n,{...e,children:(0,t.jsx)(l,{...e})}):l(e)}},8453:(e,n,o)=>{o.d(n,{R:()=>i,x:()=>a});var t=o(6540);const r={},s=t.createContext(r);function i(e){const n=t.useContext(s);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),t.createElement(s.Provider,{value:n},e.children)}}}]);