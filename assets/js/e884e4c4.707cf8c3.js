"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5019],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var r=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function p(e,t){if(null==e)return{};var n,r,i=function(e,t){if(null==e)return{};var n,r,i={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=r.createContext({}),l=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},c=function(e){var t=l(e.components);return r.createElement(s.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},f=r.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,s=e.parentName,c=p(e,["components","mdxType","originalType","parentName"]),u=l(n),f=i,m=u["".concat(s,".").concat(f)]||u[f]||d[f]||o;return n?r.createElement(m,a(a({ref:t},c),{},{components:n})):r.createElement(m,a({ref:t},c))}));function m(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,a=new Array(o);a[0]=f;var p={};for(var s in t)hasOwnProperty.call(t,s)&&(p[s]=t[s]);p.originalType=e,p[u]="string"==typeof e?e:i,a[1]=p;for(var l=2;l<o;l++)a[l]=n[l];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}f.displayName="MDXCreateElement"},505:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>u,frontMatter:()=>o,metadata:()=>p,toc:()=>l});var r=n(7462),i=(n(7294),n(3905));const o={id:"housekeeping",title:"Housekeeping"},a=void 0,p={unversionedId:"reference/housekeeping",id:"reference/housekeeping",title:"Housekeeping",description:"This page is under review and currently not visible in the menu.",source:"@site/docs/reference/housekeeping.md",sourceDirName:"reference",slug:"/reference/housekeeping",permalink:"/docs/reference/housekeeping",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/housekeeping.md",tags:[],version:"current",frontMatter:{id:"housekeeping",title:"Housekeeping"}},s={},l=[{value:"Housekeeping",id:"housekeeping",level:2}],c={toc:l};function u(e){let{components:t,...n}=e;return(0,i.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("admonition",{type:"warning"},(0,i.kt)("p",{parentName:"admonition"},"This page is under review and currently not visible in the menu.")),(0,i.kt)("h2",{id:"housekeeping"},"Housekeeping"),(0,i.kt)("p",null,"SmartDataLakeBuilder supports housekeeping for DataObjects by specifying the HousekeepingMode."),(0,i.kt)("p",null,"The following HousekeepingModes are currently implemented:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"PartitionRetentionMode: Define partitions to keep by configuring a retentionCondition.\nretentionCondition is a spark sql expression working with the attributes of PartitionExpressionData returning a boolean with value true if the partition should be kept."),(0,i.kt)("li",{parentName:"ul"},"PartitionArchiveCompactionMode: Archive and compact old partitions.",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},'Archive partition reduces the number of partitions in the past by moving older partitions into special "archive partitions".\narchiveCondition defines a spark sql expression working with the attributes of PartitionExpressionData returning archive partition values as Map',"[","String,String","]",".\nIf return value is the same as input partition values, the partition is not touched. Otherwise all files of the partition are moved to the corresponding partition.\nBe aware that the value of the partition columns changes for these files/records."),(0,i.kt)("li",{parentName:"ul"},"Compact partition reduces the number of files in a partition by rewriting them with Spark.\ncompactPartitionExpression defines a sql expression working with the attributes of PartitionExpressionData returning true if this partition should be compacted.\nOnce a partition is compacted, it is marked as compacted and will not be compacted again. It is therefore ok to return true for all partitions which should be compacted, regardless if they have been compacted already.")))),(0,i.kt)("p",null,"Example - cleanup partitions with partition layout dt=","<","yyyymmdd",">"," after 90 days:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"housekeepingMode = {\n  type = PartitionRetentionMode\n  retentionCondition = \"datediff(now(), to_date(elements['dt'], 'yyyyMMdd')) &lt= 90\"\n}\n")))}u.isMDXComponent=!0}}]);