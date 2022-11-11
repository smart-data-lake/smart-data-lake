"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2980],{3905:(e,n,t)=>{t.d(n,{Zo:()=>p,kt:()=>m});var o=t(7294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function a(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);n&&(o=o.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,o)}return t}function i(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?a(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,o,r=function(e,n){if(null==e)return{};var t,o,r={},a=Object.keys(e);for(o=0;o<a.length;o++)t=a[o],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)t=a[o],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var d=o.createContext({}),l=function(e){var n=o.useContext(d),t=n;return e&&(t="function"==typeof e?e(n):i(i({},n),e)),t},p=function(e){var n=l(e.components);return o.createElement(d.Provider,{value:n},e.children)},c={inlineCode:"code",wrapper:function(e){var n=e.children;return o.createElement(o.Fragment,{},n)}},u=o.forwardRef((function(e,n){var t=e.components,r=e.mdxType,a=e.originalType,d=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=l(t),m=r,f=u["".concat(d,".").concat(m)]||u[m]||c[m]||a;return t?o.createElement(f,i(i({ref:n},p),{},{components:t})):o.createElement(f,i({ref:n},p))}));function m(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var a=t.length,i=new Array(a);i[0]=u;var s={};for(var d in n)hasOwnProperty.call(n,d)&&(s[d]=n[d]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var l=2;l<a;l++)i[l]=t[l];return o.createElement.apply(null,i)}return o.createElement.apply(null,t)}u.displayName="MDXCreateElement"},7117:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>d,contentTitle:()=>i,default:()=>c,frontMatter:()=>a,metadata:()=>s,toc:()=>l});var o=t(7462),r=(t(7294),t(3905));const a={id:"docker-on-windows",title:"Notes for Windows Users"},i=void 0,s={unversionedId:"getting-started/troubleshooting/docker-on-windows",id:"getting-started/troubleshooting/docker-on-windows",title:"Notes for Windows Users",description:"Free Docker alternative for Windows",source:"@site/docs/getting-started/troubleshooting/docker-on-windows.md",sourceDirName:"getting-started/troubleshooting",slug:"/getting-started/troubleshooting/docker-on-windows",permalink:"/docs/getting-started/troubleshooting/docker-on-windows",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/troubleshooting/docker-on-windows.md",tags:[],version:"current",frontMatter:{id:"docker-on-windows",title:"Notes for Windows Users"},sidebar:"docs",previous:{title:"Common Problems",permalink:"/docs/getting-started/troubleshooting/common-problems"},next:{title:"Build SDL",permalink:"/docs/reference/build"}},d={},l=[{value:"Free Docker alternative for Windows",id:"free-docker-alternative-for-windows",level:2},{value:"Using podman build and podman run",id:"using-podman-build-and-podman-run",level:2},{value:"Using podman compose",id:"using-podman-compose",level:2},{value:"Known Issue with podman on WSL2 on Windows",id:"known-issue-with-podman-on-wsl2-on-windows",level:2}],p={toc:l};function c(e){let{components:n,...t}=e;return(0,r.kt)("wrapper",(0,o.Z)({},p,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"free-docker-alternative-for-windows"},"Free Docker alternative for Windows"),(0,r.kt)("p",null,"You can use Docker Desktop for Windows together with Windows command line or Windows Linux Subsystem (WSL2) for this tutorial. But note that Docker Desktop for Windows needs a license for commercial use\nbeginning of 2022."),(0,r.kt)("p",null,"There is a free alternative for Linux or WSL2 called podman from Redhat, which has a compatible command line and also the Dockerfiles are compatible, see ",(0,r.kt)("a",{parentName:"p",href:"https://podman.io/"},"podman.io"),".\nFurther advantages are that podman is more lightweight - it doesn't need a service and root privileges to run containers.\nInstall podman on WSL2 Ubuntu:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'. /etc/os-release\necho "deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/ /" | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list\ncurl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_${VERSION_ID}/Release.key" | sudo apt-key add -\nsudo apt-get update\nsudo apt-get -y upgrade\nsudo apt-get -y install podman\n')),(0,r.kt)("h2",{id:"using-podman-build-and-podman-run"},"Using podman build and podman run"),(0,r.kt)("p",null,"Throughout this tutorial, we often refer to the commands ",(0,r.kt)("inlineCode",{parentName:"p"},"docker build")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"docker run"),".\nPodman has identically named commands, which, for the purpose of this tutorial, do exactly the same thing.\nSo with podman you can just type ",(0,r.kt)("inlineCode",{parentName:"p"},"podman build")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"podman run")," instead."),(0,r.kt)("h2",{id:"using-podman-compose"},"Using podman compose"),(0,r.kt)("p",null,"For ",(0,r.kt)("a",{parentName:"p",href:"/docs/getting-started/part-2/delta-lake-format"},"part 2 of this guide"),", you need docker compose.\nFor Windows, you can use the alternative podman compose.\nInstall podman-compose for podman in WSL2:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"sudo apt install python3-pip\nsudo pip3 install podman-compose==0.1.11\n")),(0,r.kt)("admonition",{title:"podman version",type:"info"},(0,r.kt)("p",{parentName:"admonition"},(0,r.kt)("inlineCode",{parentName:"p"},"podman-compose")," with major 1 (tested up to 1.0.3) do not create pods automatically. Therewith, the used commands results in networking issues between the containers. Thus, we recommend to use the latest version with automatic pod creation, version 0.1.11. The behaviour may change in future versions. ")),(0,r.kt)("p",null,"After starting ",(0,r.kt)("inlineCode",{parentName:"p"},"podman-compose up")," in the getting-started folder you should now be able to open Polynote on port localhost:8192, as WSL2 automatically publishes all ports on Windows.\nIf the port is not accessible, you can use ",(0,r.kt)("inlineCode",{parentName:"p"},"wsl hostname -I")," on Windows command line to get the IP adress of WSL, and then access Polynote over {ip-address}:8192."),(0,r.kt)("h2",{id:"known-issue-with-podman-on-wsl2-on-windows"},"Known Issue with podman on WSL2 on Windows"),(0,r.kt)("p",null,"If you suddenly cannot execute any podman commands anymore and get an error like this:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'ERRO[0000] error joining network namespace for container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53\nError: error joining network namespace of container 88a8d5c7115598aeaa31fcd1cee8c084fee3ab2577b4f61dc317053d7da032f9: error retrieving network namespace at /tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1: unknown FS magic on "/tmp/podman-run-1000/netns/cni-f73b0b0b-155d-3c43-30b2-278280c003f1": ef53\n')),(0,r.kt)("p",null,"Then you may be experiencing a known problem with podman on WSL2 on windows after a system restart related to the /tmp directory."),(0,r.kt)("p",null,"If you encounter this error, there are two quick workarounds:"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},"Delete the tmp dir of your WSL2 installation and restart WSL2"),(0,r.kt)("li",{parentName:"ol"},"The podman commands with sudo may still work, eg. ",(0,r.kt)("inlineCode",{parentName:"li"},"sudo podman ps")," will work even if ",(0,r.kt)("inlineCode",{parentName:"li"},"podman ps")," wont")),(0,r.kt)("p",null,"See ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/containers/podman/issues/12236"},"https://github.com/containers/podman/issues/12236")," for more information."))}c.isMDXComponent=!0}}]);