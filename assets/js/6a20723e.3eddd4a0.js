"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2241],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return m}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),s=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=s(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),u=s(n),m=r,h=u["".concat(l,".").concat(m)]||u[m]||d[m]||o;return n?a.createElement(h,i(i({ref:t},p),{},{components:n})):a.createElement(h,i({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=u;var c={};for(var l in t)hasOwnProperty.call(t,l)&&(c[l]=t[l]);c.originalType=e,c.mdxType="string"==typeof e?e:r,i[1]=c;for(var s=2;s<o;s++)i[s]=n[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}u.displayName="MDXCreateElement"},8632:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return l},default:function(){return u},frontMatter:function(){return c},metadata:function(){return s},toc:function(){return p}});var a=n(7462),r=n(3366),o=(n(7294),n(3905)),i=["components"],c={id:"hoconElements",title:"Hocon Elements"},l=void 0,s={unversionedId:"reference/hoconElements",id:"reference/hoconElements",title:"Hocon Elements",description:"This page is under review and currently not visible in the menu.",source:"@site/docs/reference/hoconElements.md",sourceDirName:"reference",slug:"/reference/hoconElements",permalink:"/docs/reference/hoconElements",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/hoconElements.md",tags:[],version:"current",frontMatter:{id:"hoconElements",title:"Hocon Elements"}},p=[{value:"Connections",id:"connections",children:[],level:2},{value:"Data Objects",id:"data-objects",children:[],level:2},{value:"Actions",id:"actions",children:[],level:2}],d={toc:p};function u(e){var t=e.components,c=(0,r.Z)(e,i);return(0,o.kt)("wrapper",(0,a.Z)({},d,c,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("div",{className:"admonition admonition-warning alert alert--danger"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M5.05.31c.81 2.17.41 3.38-.52 4.31C3.55 5.67 1.98 6.45.9 7.98c-1.45 2.05-1.7 6.53 3.53 7.7-2.2-1.16-2.67-4.52-.3-6.61-.61 2.03.53 3.33 1.94 2.86 1.39-.47 2.3.53 2.27 1.67-.02.78-.31 1.44-1.13 1.81 3.42-.59 4.78-3.42 4.78-5.56 0-2.84-2.53-3.22-1.25-5.61-1.52.13-2.03 1.13-1.89 2.75.09 1.08-1.02 1.8-1.86 1.33-.67-.41-.66-1.19-.06-1.78C8.18 5.31 8.68 2.45 5.05.32L5.03.3l.02.01z"}))),"warning")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"This page is under review and currently not visible in the menu."))),(0,o.kt)("h2",{id:"connections"},"Connections"),(0,o.kt)("p",null,"Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a database.\nInstead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.\nThe possible parameters depend on the connection type. Please note the section on ",(0,o.kt)("a",{parentName:"p",href:"#user-and-password-variables"},"usernames and password"),"."),(0,o.kt)("p",null,"For a list of all available connections, please consult the ",(0,o.kt)("a",{parentName:"p",href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=1"},"Configuration Schema Browser")," directly."),(0,o.kt)("p",null,"In the package overview, you can also see the parameters available to each type of connection and which parameters are optional."),(0,o.kt)("h2",{id:"data-objects"},"Data Objects"),(0,o.kt)("p",null,"For a list of all available data objects, please consult the ",(0,o.kt)("a",{parentName:"p",href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2"},"Configuration Schema Browser")," directly.\nIn the package overview, you can also see the parameters available to each type of data object and which parameters are optional."),(0,o.kt)("p",null,"Data objects are structured in a hierarchy as many attributes are shared between them, i.e. do Hive tables and transactional tables share common attributes modeled in TableDataObject."),(0,o.kt)("p",null,"Here is an overview of all data objects:\n",(0,o.kt)("img",{alt:"data object hierarchy",src:n(8949).Z,width:"1334",height:"1032"})),(0,o.kt)("h2",{id:"actions"},"Actions"),(0,o.kt)("p",null,"For a list of all available actions, please consult the ",(0,o.kt)("a",{parentName:"p",href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=3"},"Configuration Schema Browser")," directly."),(0,o.kt)("p",null,"In the package overview, you can also see the parameters available to each type of action and which parameters are optional."))}u.isMDXComponent=!0},8949:function(e,t,n){t.Z=n.p+"assets/images/dataobject_hierarchy-1fcc5e3f99c4affc55451618cd99fd53.png"}}]);