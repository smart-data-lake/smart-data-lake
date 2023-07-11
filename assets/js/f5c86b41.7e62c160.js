"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3227],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),c=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=c(n),m=a,h=u["".concat(l,".").concat(m)]||u[m]||d[m]||o;return n?r.createElement(h,i(i({ref:t},p),{},{components:n})):r.createElement(h,i({ref:t},p))}));function h(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[u]="string"==typeof e?e:a,i[1]=s;for(var c=2;c<o;c++)i[c]=n[c];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},3735:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>s,toc:()=>c});var r=n(7462),a=(n(7294),n(3905));const o={title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},i=void 0,s={permalink:"/blog/sdl-hist",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",source:"@site/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",date:"2022-05-11T00:00:00.000Z",formattedDate:"May 11, 2022",tags:[{label:"historization",permalink:"/blog/tags/historization"},{label:"MSSQL",permalink:"/blog/tags/mssql"},{label:"incremental",permalink:"/blog/tags/incremental"},{label:"CDC",permalink:"/blog/tags/cdc"}],readingTime:12.495,hasTruncateMarker:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},prevItem:{title:"Housekeeping",permalink:"/blog/sdl-housekeeping"},nextItem:{title:"Deployment on Databricks",permalink:"/blog/sdl-databricks"}},l={authorsImageUrls:[void 0]},c=[],p={toc:c},u="wrapper";function d(e){let{components:t,...n}=e;return(0,a.kt)(u,(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"In many cases datasets have no constant live. New data points are created, values changed and data expires. We are interested in keeping track of all these changes.\nThis article first presents collecting data utilizing ",(0,a.kt)("strong",{parentName:"p"},"JDBC")," and ",(0,a.kt)("strong",{parentName:"p"},"deduplication on the fly"),". Then, a ",(0,a.kt)("strong",{parentName:"p"},"Change Data Capture")," (CDC) enabled (MS)SQL table will be transferred and historized in the data lake using the ",(0,a.kt)("strong",{parentName:"p"},"Airbyte MS SQL connector")," supporting CDC. Methods for reducing the computational and storage efforts are mentioned."))}d.isMDXComponent=!0}}]);