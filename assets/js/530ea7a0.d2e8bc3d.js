"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[459],{3905:function(e,t,a){a.d(t,{Zo:function(){return d},kt:function(){return m}});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),u=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},d=function(e){var t=u(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},p=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,d=i(e,["components","mdxType","originalType","parentName"]),p=u(a),m=r,h=p["".concat(s,".").concat(m)]||p[m]||c[m]||o;return a?n.createElement(h,l(l({ref:t},d),{},{components:a})):n.createElement(h,l({ref:t},d))}));function m(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,l=new Array(o);l[0]=p;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:r,l[1]=i;for(var u=2;u<o;u++)l[u]=a[u];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}p.displayName="MDXCreateElement"},8215:function(e,t,a){var n=a(7294);t.Z=function(e){var t=e.children,a=e.hidden,r=e.className;return n.createElement("div",{role:"tabpanel",hidden:a,className:r},t)}},9877:function(e,t,a){a.d(t,{Z:function(){return d}});var n=a(7462),r=a(7294),o=a(2389),l=a(9548),i=a(6010),s="tabItem_LplD";function u(e){var t,a,o,u=e.lazy,d=e.block,c=e.defaultValue,p=e.values,m=e.groupId,h=e.className,v=r.Children.map(e.children,(function(e){if((0,r.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})),k=null!=p?p:v.map((function(e){var t=e.props;return{value:t.value,label:t.label,attributes:t.attributes}})),f=(0,l.lx)(k,(function(e,t){return e.value===t.value}));if(f.length>0)throw new Error('Docusaurus error: Duplicate values "'+f.map((function(e){return e.value})).join(", ")+'" found in <Tabs>. Every value needs to be unique.');var g=null===c?c:null!=(t=null!=c?c:null==(a=v.find((function(e){return e.props.default})))?void 0:a.props.value)?t:null==(o=v[0])?void 0:o.props.value;if(null!==g&&!k.some((function(e){return e.value===g})))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+g+'" but none of its children has the corresponding value. Available values are: '+k.map((function(e){return e.value})).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");var b=(0,l.UB)(),w=b.tabGroupChoices,y=b.setTabGroupChoices,N=(0,r.useState)(g),D=N[0],x=N[1],T=[],j=(0,l.o5)().blockElementScrollPositionUntilNextRender;if(null!=m){var O=w[m];null!=O&&O!==D&&k.some((function(e){return e.value===O}))&&x(O)}var P=function(e){var t=e.currentTarget,a=T.indexOf(t),n=k[a].value;n!==D&&(j(t),x(n),null!=m&&y(m,n))},S=function(e){var t,a=null;switch(e.key){case"ArrowRight":var n=T.indexOf(e.currentTarget)+1;a=T[n]||T[0];break;case"ArrowLeft":var r=T.indexOf(e.currentTarget)-1;a=T[r]||T[T.length-1]}null==(t=a)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":d},h)},k.map((function(e){var t=e.value,a=e.label,o=e.attributes;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:D===t?0:-1,"aria-selected":D===t,key:t,ref:function(e){return T.push(e)},onKeyDown:S,onFocus:P,onClick:P},o,{className:(0,i.Z)("tabs__item",s,null==o?void 0:o.className,{"tabs__item--active":D===t})}),null!=a?a:t)}))),u?(0,r.cloneElement)(v.filter((function(e){return e.props.value===D}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},v.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==D})}))))}function d(e){var t=(0,o.Z)();return r.createElement(u,(0,n.Z)({key:String(t)},e))}},3148:function(e,t,a){a.r(t),a.d(t,{contentTitle:function(){return d},default:function(){return h},frontMatter:function(){return u},metadata:function(){return c},toc:function(){return p}});var n=a(7462),r=a(3366),o=(a(7294),a(3905)),l=a(9877),i=a(8215),s=["components"],u={id:"setup",title:"Technical Setup"},d=void 0,c={unversionedId:"getting-started/setup",id:"getting-started/setup",title:"Technical Setup",description:"Requirements",source:"@site/docs/getting-started/setup.md",sourceDirName:"getting-started",slug:"/getting-started/setup",permalink:"/docs/getting-started/setup",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/setup.md",tags:[],version:"current",frontMatter:{id:"setup",title:"Technical Setup"},sidebar:"docs",previous:{title:"Architecture",permalink:"/docs/architecture"},next:{title:"Inputs",permalink:"/docs/getting-started/get-input-data"}},p=[{value:"Requirements",id:"requirements",children:[],level:2},{value:"Build Spark docker image",id:"build-spark-docker-image",children:[],level:2},{value:"Compile Scala Classes",id:"compile-scala-classes",children:[],level:2},{value:"Run SDL with Spark docker image",id:"run-sdl-with-spark-docker-image",children:[],level:2},{value:"Development Environment",id:"development-environment",children:[{value:"Hadoop Setup (Needed for Windows only)",id:"hadoop-setup-needed-for-windows-only",children:[],level:3},{value:"Run SDL in IntelliJ",id:"run-sdl-in-intellij",children:[],level:3}],level:2}],m={toc:p};function h(e){var t=e.components,a=(0,r.Z)(e,s);return(0,o.kt)("wrapper",(0,n.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"requirements"},"Requirements"),(0,o.kt)("p",null,"To run this tutorial you just need two things:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://www.docker.com/get-started"},"Docker"),", including docker-compose. If you use Windows, you might want to use ",(0,o.kt)("a",{parentName:"li",href:"/docs/getting-started/troubleshooting/docker-on-windows"},"podman as an alternative to docker"),"."),(0,o.kt)("li",{parentName:"ul"},"The ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started"},"source code of the example"),".")),(0,o.kt)("h2",{id:"build-spark-docker-image"},"Build Spark docker image"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Download the source code of the example either via git or by ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip"},"downloading the zip")," and extracting it."),(0,o.kt)("li",{parentName:"ul"},"Open up a terminal and change to the folder with the source, you should see a file called Dockerfile. "),(0,o.kt)("li",{parentName:"ul"},"Then run (note: this might take some time, but it's only needed once):")),(0,o.kt)(l.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"docker build -t sdl-spark .\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"podman build -t sdl-spark .\n")))),(0,o.kt)("h2",{id:"compile-scala-classes"},"Compile Scala Classes"),(0,o.kt)("p",null,"Utilizing a Maven container, the getting-started project with the required SDL Scala sources and all required libraries are compiled and packed using the following command.  "),(0,o.kt)(l.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},'mkdir .mvnrepo\ndocker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n')))),(0,o.kt)("div",{className:"admonition admonition-note alert alert--secondary"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.3 5.69a.942.942 0 0 1-.28-.7c0-.28.09-.52.28-.7.19-.18.42-.28.7-.28.28 0 .52.09.7.28.18.19.28.42.28.7 0 .28-.09.52-.28.7a1 1 0 0 1-.7.3c-.28 0-.52-.11-.7-.3zM8 7.99c-.02-.25-.11-.48-.31-.69-.2-.19-.42-.3-.69-.31H6c-.27.02-.48.13-.69.31-.2.2-.3.44-.31.69h1v3c.02.27.11.5.31.69.2.2.42.31.69.31h1c.27 0 .48-.11.69-.31.2-.19.3-.42.31-.69H8V7.98v.01zM7 2.3c-3.14 0-5.7 2.54-5.7 5.68 0 3.14 2.56 5.7 5.7 5.7s5.7-2.55 5.7-5.7c0-3.15-2.56-5.69-5.7-5.69v.01zM7 .98c3.86 0 7 3.14 7 7s-3.14 7-7 7-7-3.12-7-7 3.14-7 7-7z"}))),"note")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"This might take some time, but it's only needed at the beginning or if Scala code has changed."))),(0,o.kt)("h2",{id:"run-sdl-with-spark-docker-image"},"Run SDL with Spark docker image"),(0,o.kt)("p",null,"Now let's see Smart Data Lake in action!"),(0,o.kt)(l.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir data\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir data\npodman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n")))),(0,o.kt)("p",null,"This creates a folder in the current directory named ",(0,o.kt)("em",{parentName:"p"},"data")," and then\nexecutes a simple data pipeline that downloads two files from two different websites into that directory."),(0,o.kt)("p",null,"When the execution is complete, you should see the two files in the ",(0,o.kt)("em",{parentName:"p"},"data")," folder.\nWonder what happened ? You will create the data pipeline that does just this in the first steps of this guide."),(0,o.kt)("p",null,"If you wish, you can start with ",(0,o.kt)("a",{parentName:"p",href:"get-input-data"},"part 1")," right away.\nFor ",(0,o.kt)("a",{parentName:"p",href:"/docs/getting-started/part-2/industrializing"},"part 2")," and ",(0,o.kt)("a",{parentName:"p",href:"/docs/getting-started/part-3/custom-webservice"},"part 3"),", it is recommended to setup a Development Environment."),(0,o.kt)("h2",{id:"development-environment"},"Development Environment"),(0,o.kt)("p",null,"For some parts of this tutorial it is beneficial to have a working development environment ready. In the following we will mainly explain how one can configure a working evironment for\nWindows or Linux. We will focus on the community version of Intellij. Please ",(0,o.kt)("a",{parentName:"p",href:"https://www.jetbrains.com/idea/"},"download")," the version that suits your operating system. "),(0,o.kt)("h3",{id:"hadoop-setup-needed-for-windows-only"},"Hadoop Setup (Needed for Windows only)"),(0,o.kt)("p",null,"Windows Users need to follow the steps below to have a working Hadoop Installation :"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"First download the Windows binaries for Hadoop ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/cdarlint/winutils/archive/refs/heads/master.zip"},"here")),(0,o.kt)("li",{parentName:"ol"},"Extract the wished version to a folder (e.g. ","<"," prefix ",">","\\hadoop-","<"," version ",">","\\bin ). For this tutorial we use the version 3.2.2."),(0,o.kt)("li",{parentName:"ol"},"Configure the ",(0,o.kt)("em",{parentName:"li"},"HADOOP_HOME")," environment variable to point to the folder ","<"," prefix ",">","\\hadoop-","<"," version ",">","\\"),(0,o.kt)("li",{parentName:"ol"},"Add the ",(0,o.kt)("em",{parentName:"li"},"%HADOOP_HOME%\\bin")," to the ",(0,o.kt)("em",{parentName:"li"},"PATH")," environment variable")),(0,o.kt)("h3",{id:"run-sdl-in-intellij"},"Run SDL in IntelliJ"),(0,o.kt)("p",null,"We will focus on the community version of Intellij. Please ",(0,o.kt)("a",{parentName:"p",href:"https://www.jetbrains.com/idea/"},"download")," the version that suits your operating system.\nThis needs an Intellij and Java SDK installation. Please make sure you have:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Java 8 SDK or Java 11 SDK"),(0,o.kt)("li",{parentName:"ul"},"Scala Version 2.12. You need to install the Scala-Plugin with this exact version and DO NOT UPGRADE to Scala 3. For the complete list of versions at play in SDLB, ",(0,o.kt)("a",{parentName:"li",href:"../reference/build"},"you can consult the Reference"),".")),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Load the project as a maven project: Right-click on pom.xml file -> add as Maven Project"),(0,o.kt)("li",{parentName:"ol"},"Ensure all correct dependencies are loaded: Right-click on pom.xml file, Maven -> Reload Project"),(0,o.kt)("li",{parentName:"ol"},"Configure and run the following run configuration in IntelliJ IDEA:",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Main class: ",(0,o.kt)("inlineCode",{parentName:"li"},"io.smartdatalake.app.LocalSmartDataLakeBuilder")),(0,o.kt)("li",{parentName:"ul"},"Program arguments: ",(0,o.kt)("inlineCode",{parentName:"li"},"--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/config")),(0,o.kt)("li",{parentName:"ul"},"Working directory: ",(0,o.kt)("inlineCode",{parentName:"li"},"/path/to/sdl-examples/target")," or just ",(0,o.kt)("inlineCode",{parentName:"li"},"target"))))),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Congratulations!")," You're now all setup! Head over to the next step to analyse these files..."))}h.isMDXComponent=!0}}]);