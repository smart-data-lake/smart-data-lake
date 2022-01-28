"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[404],{3905:function(t,e,n){n.d(e,{Zo:function(){return d},kt:function(){return h}});var r=n(7294);function a(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function i(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function o(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?i(Object(n),!0).forEach((function(e){a(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function s(t,e){if(null==t)return{};var n,r,a=function(t,e){if(null==t)return{};var n,r,a={},i=Object.keys(t);for(r=0;r<i.length;r++)n=i[r],e.indexOf(n)>=0||(a[n]=t[n]);return a}(t,e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(t);for(r=0;r<i.length;r++)n=i[r],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(a[n]=t[n])}return a}var p=r.createContext({}),l=function(t){var e=r.useContext(p),n=e;return t&&(n="function"==typeof t?t(e):o(o({},e),t)),n},d=function(t){var e=l(t.components);return r.createElement(p.Provider,{value:e},t.children)},u={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},c=r.forwardRef((function(t,e){var n=t.components,a=t.mdxType,i=t.originalType,p=t.parentName,d=s(t,["components","mdxType","originalType","parentName"]),c=l(n),h=a,m=c["".concat(p,".").concat(h)]||c[h]||u[h]||i;return n?r.createElement(m,o(o({ref:e},d),{},{components:n})):r.createElement(m,o({ref:e},d))}));function h(t,e){var n=arguments,a=e&&e.mdxType;if("string"==typeof t||a){var i=n.length,o=new Array(i);o[0]=c;var s={};for(var p in e)hasOwnProperty.call(e,p)&&(s[p]=e[p]);s.originalType=t,s.mdxType="string"==typeof t?t:a,o[1]=s;for(var l=2;l<i;l++)o[l]=n[l];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}c.displayName="MDXCreateElement"},4334:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return s},contentTitle:function(){return p},metadata:function(){return l},toc:function(){return d},default:function(){return c}});var r=n(7462),a=n(3366),i=(n(7294),n(3905)),o=["components"],s={title:"Inputs"},p=void 0,l={unversionedId:"getting-started/get-input-data",id:"getting-started/get-input-data",title:"Inputs",description:"Goal",source:"@site/docs/getting-started/get-input-data.md",sourceDirName:"getting-started",slug:"/getting-started/get-input-data",permalink:"/docs/getting-started/get-input-data",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/get-input-data.md",tags:[],version:"current",frontMatter:{title:"Inputs"},sidebar:"docs",previous:{title:"Technical Setup",permalink:"/docs/getting-started/setup"},next:{title:"Get Departures",permalink:"/docs/getting-started/part-1/get-departures"}},d=[{value:"Goal",id:"goal",children:[],level:2},{value:"Our Input Data",id:"our-input-data",children:[{value:"departures",id:"departures",children:[],level:3},{value:"airports.csv",id:"airportscsv",children:[],level:3}],level:2},{value:"Next step",id:"next-step",children:[],level:2}],u={toc:d};function c(t){var e=t.components,n=(0,a.Z)(t,o);return(0,i.kt)("wrapper",(0,r.Z)({},u,n,{components:e,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"goal"},"Goal"),(0,i.kt)("p",null,"Let's say your friend Tom is a fan of railways and he lives next to an airport.\nHe wonders how many flights that start from his neighborhood could be replaced by rail traffic.\nFor that he would need to find out which flights depart from his airport, as well as how far they are flying.\nIf they fly less than, say 500 km, then that would be a journey that could be done by rail."),(0,i.kt)("p",null,"You just discovered this tool called Smart Data Lake Builder that's supposedly good for combining data from different sources and performing some analysis on it.\nSo you decide to help Tom by trying that framework."),(0,i.kt)("h2",{id:"our-input-data"},"Our Input Data"),(0,i.kt)("p",null,"Our first step is to get the input data.\nAfter browsing the web a bit, you end up finding a website that looks promising."),(0,i.kt)("h3",{id:"departures"},"departures"),(0,i.kt)("p",null,"The site is called ",(0,i.kt)("a",{parentName:"p",href:"https://openskynetwork.github.io/opensky-api/rest.html#id17"},"openskynetwork"),"\nand it provides you with a free REST-Interface for getting departures by airport.\nNotice that you need the ICAO identifier of Tom's airport to get the right parameters.\nYou know that Tom lives near Bern, Switzerland. A quick web search shows you that the identifier is\n",(0,i.kt)("em",{parentName:"p"},"LSZB"),". Let's focus on some specific time period for now to have reproducible results.\nYou end up with the following REST-URL:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979\n")),(0,i.kt)("p",null,"When you run this in your web-browser, you will get a response in the JSON Format.\nFor each record, it contains the ICAO identifier of the airport where the plane is flying to in the field\n",(0,i.kt)("em",{parentName:"p"},"estArrivalAirport"),". That's a good start! "),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Notice that the result of this JSON-call is exactly what was downloaded in the previous step into\nthe folder ",(0,i.kt)("em",{parentName:"p"},"data/ext_departures/result.json"),"."))),(0,i.kt)("h3",{id:"airportscsv"},"airports.csv"),(0,i.kt)("p",null,"Now you need some kind of list of all airports with their respective locations.\nYou end up finding a ",(0,i.kt)("a",{parentName:"p",href:"https://ourairports.com/data/"},"website")," that has just that!\nIt hosts a csv-file called ",(0,i.kt)("em",{parentName:"p"},"airports.csv")," which contains what you need."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Notice that this CSV-File is exactly what was downloaded in the previous step into\nthe folder ",(0,i.kt)("em",{parentName:"p"},"data/ext_airports/result.csv"),"."))),(0,i.kt)("h2",{id:"next-step"},"Next step"),(0,i.kt)("p",null,"Now that we know our input data, we can start our analysis.\nIn the next step, we will start ",(0,i.kt)("em",{parentName:"p"},"Part 1 of the Getting Started Guide"),"\nto do our first steps with Smart Data Lake Builder."))}c.isMDXComponent=!0}}]);