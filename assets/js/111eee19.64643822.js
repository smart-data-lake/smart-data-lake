"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2114],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return m}});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},i=Object.keys(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=o.createContext({}),p=function(e){var t=o.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},d=function(e){var t=p(e.components);return o.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},u=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),u=p(n),m=r,h=u["".concat(s,".").concat(m)]||u[m]||c[m]||i;return n?o.createElement(h,a(a({ref:t},d),{},{components:n})):o.createElement(h,a({ref:t},d))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,a=new Array(i);a[0]=u;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,a[1]=l;for(var p=2;p<i;p++)a[p]=n[p];return o.createElement.apply(null,a)}return o.createElement.apply(null,n)}u.displayName="MDXCreateElement"},2722:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return s},default:function(){return u},frontMatter:function(){return l},metadata:function(){return p},toc:function(){return d}});var o=n(7462),r=n(3366),i=(n(7294),n(3905)),a=["components"],l={id:"troubleshooting",title:"Troubleshooting"},s=void 0,p={unversionedId:"reference/troubleshooting",id:"reference/troubleshooting",title:"Troubleshooting",description:"If you have problems with the getting started guide, note that there's a separate troubleshooting section for that.",source:"@site/docs/reference/troubleshooting.md",sourceDirName:"reference",slug:"/reference/troubleshooting",permalink:"/docs/reference/troubleshooting",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/troubleshooting.md",tags:[],version:"current",frontMatter:{id:"troubleshooting",title:"Troubleshooting"},sidebar:"docs",previous:{title:"Execution Engines",permalink:"/docs/reference/executionEngines"}},d=[{value:"Windows: missing winutils",id:"windows-missing-winutils",children:[],level:2},{value:"Windows: <code>/tmp/hive</code> is not writable",id:"windows-tmphive-is-not-writable",children:[],level:2},{value:"Windows: winutils.exe is not working correctly",id:"windows-winutilsexe-is-not-working-correctly",children:[],level:2},{value:"Resources not copied",id:"resources-not-copied",children:[],level:2},{value:"Maven compile error: tools.jar",id:"maven-compile-error-toolsjar",children:[],level:2},{value:"How can I test Hadoop / HDFS locally ?",id:"how-can-i-test-hadoop--hdfs-locally-",children:[],level:2}],c={toc:d};function u(e){var t=e.components,n=(0,r.Z)(e,a);return(0,i.kt)("wrapper",(0,o.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"If you have problems with the getting started guide, note that there's a separate ",(0,i.kt)("a",{parentName:"p",href:"/docs/getting-started/troubleshooting/common-problems"},"troubleshooting section")," for that."))),(0,i.kt)("h2",{id:"windows-missing-winutils"},"Windows: missing winutils"),(0,i.kt)("p",null,"Error:",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("inlineCode",{parentName:"p"},"java.io.IOException: Could not locate executable null\\bin\\winutils.exe in the Hadoop binaries")),(0,i.kt)("p",null,"Cause:",(0,i.kt)("br",{parentName:"p"}),"\n","The ",(0,i.kt)("inlineCode",{parentName:"p"},"winutils.exe")," executable can not be found."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Download hadoop winutils binaries (e.g ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/cdarlint/winutils/archive/refs/heads/master.zip"},"https://github.com/cdarlint/winutils/archive/refs/heads/master.zip"),")"),(0,i.kt)("li",{parentName:"ul"},"Extract binaries for desired hadoop version into folder (e.g. hadoop-3.2.2\\bin)"),(0,i.kt)("li",{parentName:"ul"},"Set HADOOP_HOME evironment variable (e.g. HADOOP_HOME=...\\hadoop-3.2.2).\nNote that the binary files need to be located at %HADOOP_HOME%\\bin!"),(0,i.kt)("li",{parentName:"ul"},"Add %HADOOP_HOME%\\bin to PATH variable.")),(0,i.kt)("h2",{id:"windows-tmphive-is-not-writable"},"Windows: ",(0,i.kt)("inlineCode",{parentName:"h2"},"/tmp/hive")," is not writable"),(0,i.kt)("p",null,"Error:",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("inlineCode",{parentName:"p"},"RuntimeException: Error while running command to get file permissions"),(0,i.kt)("br",{parentName:"p"}),"\n","Solution:",(0,i.kt)("br",{parentName:"p"}),"\n","Change to ",(0,i.kt)("inlineCode",{parentName:"p"},"%HADOOP_HOME%\\bin")," and execute ",(0,i.kt)("inlineCode",{parentName:"p"},"winutils chmod 777 /tmp/hive"),"."),(0,i.kt)("h2",{id:"windows-winutilsexe-is-not-working-correctly"},"Windows: winutils.exe is not working correctly"),(0,i.kt)("p",null,"Error:",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("inlineCode",{parentName:"p"},"winutils.exe - System Error The code execution cannot proceed because MSVCR100.dll was not found. Reinstalling the program may fix this problem."),"  "),(0,i.kt)("p",null,"Other errors are also possible:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Similar error message when double clicking on winutils.exe (Popup)"),(0,i.kt)("li",{parentName:"ul"},"Errors when providing a path to the configuration instead of a single configuration file"),(0,i.kt)("li",{parentName:"ul"},"ExitCodeException exitCode=-1073741515 when executing SDL even though everything ran without errors")),(0,i.kt)("p",null,"Solution:",(0,i.kt)("br",{parentName:"p"}),"\n","Install VC++ Redistributable Package from Microsoft:",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("a",{parentName:"p",href:"http://www.microsoft.com/en-us/download/details.aspx?id=5555"},"http://www.microsoft.com/en-us/download/details.aspx?id=5555")," (x86)",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("a",{parentName:"p",href:"http://www.microsoft.com/en-us/download/details.aspx?id=14632"},"http://www.microsoft.com/en-us/download/details.aspx?id=14632")," (x64)"),(0,i.kt)("h2",{id:"resources-not-copied"},"Resources not copied"),(0,i.kt)("p",null,"Symptom:",(0,i.kt)("br",{parentName:"p"}),"\n","Tests fail due to missing or outdated resources or the execution starts but can not find the feeds specified.\nIntelliJ might not copy the resource files to the target directory."),(0,i.kt)("p",null,"Solution:",(0,i.kt)("br",{parentName:"p"}),"\n","Execute the maven goal ",(0,i.kt)("inlineCode",{parentName:"p"},"resources:resources")," (",(0,i.kt)("inlineCode",{parentName:"p"},"mvn resources:resources"),") manually after you changed any resource file."),(0,i.kt)("h2",{id:"maven-compile-error-toolsjar"},"Maven compile error: tools.jar"),(0,i.kt)("p",null,"Error:",(0,i.kt)("br",{parentName:"p"}),"\n",(0,i.kt)("inlineCode",{parentName:"p"},"Could not find artifact jdk.tools:jdk.tools:jar:1.7 at specified path ...")),(0,i.kt)("p",null,"Context:",(0,i.kt)("br",{parentName:"p"}),"\n","Hadoop/Spark has a dependency on the tools.jar file which is installed as part of the JDK installation."),(0,i.kt)("p",null,"Possible Reasons:  "),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"Your system does not have a JDK installed (only a JRE).",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Fix: Make sure a JDK is installed and your PATH and JAVA_HOME environment variables are pointing to the JDK installation."))),(0,i.kt)("li",{parentName:"ol"},"You are using a Java 9 JDK or higher. The tools.jar has been removed in JDK 9. See: ",(0,i.kt)("a",{parentName:"li",href:"https://openjdk.java.net/jeps/220"},"https://openjdk.java.net/jeps/220"),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Fix: Downgrade your JDK to Java 8.")))),(0,i.kt)("h2",{id:"how-can-i-test-hadoop--hdfs-locally-"},"How can I test Hadoop / HDFS locally ?"),(0,i.kt)("p",null,"When using ",(0,i.kt)("inlineCode",{parentName:"p"},"local://")," URIs, file permissions on Windows, or certain actions, local Hadoop binaries are required."),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"Download your desired Apache Hadoop binary release from ",(0,i.kt)("a",{parentName:"li",href:"https://hadoop.apache.org/releases.html"},"https://hadoop.apache.org/releases.html"),"."),(0,i.kt)("li",{parentName:"ol"},"Extract the contents of the Hadoop distribution archive to a location of your choice, e.g., ",(0,i.kt)("inlineCode",{parentName:"li"},"/path/to/hadoop")," (Unix) or ",(0,i.kt)("inlineCode",{parentName:"li"},"C:\\path\\to\\hadoop")," (Windows)."),(0,i.kt)("li",{parentName:"ol"},"Set the environment variable ",(0,i.kt)("inlineCode",{parentName:"li"},"HADOOP_HOME=/path/to/hadoop")," (Unix) or ",(0,i.kt)("inlineCode",{parentName:"li"},"HADOOP_HOME=C:\\path\\to\\hadoop")," (Windows)."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"Windows only"),": Download a Hadoop winutils distribution corresponding to your Hadoop version from ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/steveloughran/winutils"},"https://github.com/steveloughran/winutils")," (for newer Hadoop releases at: ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/cdarlint/winutils"},"https://github.com/cdarlint/winutils"),") and extract the contents to ",(0,i.kt)("inlineCode",{parentName:"li"},"%HADOOP_HOME%\\bin"),".")))}u.isMDXComponent=!0}}]);