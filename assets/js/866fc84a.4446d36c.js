"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[9233],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return m}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),s=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=s(e.components);return n.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),d=s(r),m=a,b=d["".concat(l,".").concat(m)]||d[m]||u[m]||o;return r?n.createElement(b,i(i({ref:t},p),{},{components:r})):n.createElement(b,i({ref:t},p))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=d;var c={};for(var l in t)hasOwnProperty.call(t,l)&&(c[l]=t[l]);c.originalType=e,c.mdxType="string"==typeof e?e:a,i[1]=c;for(var s=2;s<o;s++)i[s]=r[s];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},3190:function(e,t,r){r.r(t),r.d(t,{assets:function(){return p},contentTitle:function(){return l},default:function(){return m},frontMatter:function(){return c},metadata:function(){return s},toc:function(){return u}});var n=r(7462),a=r(3366),o=(r(7294),r(3905)),i=["components"],c={title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},l=void 0,s={permalink:"/blog/sdl-databricks",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",source:"@site/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",date:"2022-04-07T00:00:00.000Z",formattedDate:"April 7, 2022",tags:[{label:"Databricks",permalink:"/blog/tags/databricks"},{label:"Cloud",permalink:"/blog/tags/cloud"}],readingTime:5.74,truncated:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},prevItem:{title:"Incremental historization using CDC and Airbyte MSSQL connector",permalink:"/blog/sdl-hist"},nextItem:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",permalink:"/blog/sdl-snowpark"}},p={authorsImageUrls:[void 0]},u=[],d={toc:u};function m(e){var t=e.components,r=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"Many analytics applications are ported to the cloud, Data Lakes and Lakehouses in the cloud becoming more and more popular.\nThe ",(0,o.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," platform provides an easy accessible and easy configurable way to implement a modern analytics platform.\nSmart Data Lake Builder on the other hand provides an open source, portable automation tool to load and transform the data."),(0,o.kt)("p",null,"In this article the deployment of Smart Data Lake Builder (SDLB) on ",(0,o.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," is described."))}m.isMDXComponent=!0}}]);