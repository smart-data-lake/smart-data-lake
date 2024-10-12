"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3755],{7809:(t,e,n)=>{n.r(e),n.d(e,{assets:()=>d,contentTitle:()=>s,default:()=>p,frontMatter:()=>a,metadata:()=>o,toc:()=>l});var r=n(4848),i=n(8453);const a={title:"Inputs"},s=void 0,o={id:"getting-started/get-input-data",title:"Inputs",description:"Goal",source:"@site/docs/getting-started/get-input-data.md",sourceDirName:"getting-started",slug:"/getting-started/get-input-data",permalink:"/docs/getting-started/get-input-data",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/get-input-data.md",tags:[],version:"current",frontMatter:{title:"Inputs"},sidebar:"tutorialSidebar",previous:{title:"Technical Setup",permalink:"/docs/getting-started/setup"},next:{title:"Get Departures",permalink:"/docs/getting-started/part-1/get-departures"}},d={},l=[{value:"Goal",id:"goal",level:2},{value:"Our Input Data",id:"our-input-data",level:2},{value:"departures",id:"departures",level:3},{value:"airports.csv",id:"airportscsv",level:3},{value:"Next step",id:"next-step",level:2}];function h(t){const e={a:"a",admonition:"admonition",em:"em",h2:"h2",h3:"h3",p:"p",...(0,i.R)(),...t.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(e.h2,{id:"goal",children:"Goal"}),"\n",(0,r.jsx)(e.p,{children:"Let's say your friend Tom is a fan of railways, and he lives next to an airport.\nHe wonders how many flights that start from his neighborhood could be replaced by rail traffic.\nFor that he would need to find out which flights depart from his airport, as well as how far they are flying.\nIf they fly less than, say 500 km, then that would be a journey that could be done by rail."}),"\n",(0,r.jsx)(e.p,{children:"You just discovered this tool called Smart Data Lake Builder that's supposedly good for combining data from different sources and performing some analysis on it.\nSo you decide to help Tom by trying that framework."}),"\n",(0,r.jsx)(e.h2,{id:"our-input-data",children:"Our Input Data"}),"\n",(0,r.jsx)(e.p,{children:"Our first step is to get the input data.\nAfter browsing the web a bit, you end up finding a website that looks promising."}),"\n",(0,r.jsx)(e.h3,{id:"departures",children:"departures"}),"\n",(0,r.jsxs)(e.p,{children:["The site is called ",(0,r.jsx)(e.a,{href:"https://openskynetwork.github.io/opensky-api/rest.html#id17",children:"openskynetwork"}),",\nand it provides you with a free REST-Interface for getting departures by airport.\nNotice that you need the ICAO identifier of Tom's airport to get the right parameters.\nYou know that Tom lives near Bern, Switzerland. A quick web search shows you that the identifier is\n",(0,r.jsx)(e.em,{children:"LSZB"}),". Let's focus on some specific time period for now to have reproducible results.\nYou end up with the following REST-URL:"]}),"\n",(0,r.jsx)(e.p,{children:(0,r.jsx)(e.a,{href:"https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653",children:"https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653"})}),"\n",(0,r.jsxs)(e.p,{children:["When you run this in your web-browser, you will get a response in the JSON Format.\nFor each record, it contains the ICAO identifier of the airport where the plane is flying to in the field\n",(0,r.jsx)(e.em,{children:"estArrivalAirport"}),". That's a good start!"]}),"\n",(0,r.jsx)(e.admonition,{type:"info",children:(0,r.jsxs)(e.p,{children:["Notice that the result of this JSON-call is exactly what was downloaded in the previous step into\nthe folder ",(0,r.jsx)(e.em,{children:"data/stg_departures/result.json"}),"."]})}),"\n",(0,r.jsx)(e.h3,{id:"airportscsv",children:"airports.csv"}),"\n",(0,r.jsxs)(e.p,{children:["Now you need some kind of list of all airports with their respective locations.\nYou end up finding a ",(0,r.jsx)(e.a,{href:"https://ourairports.com/data/",children:"website"})," that has just that!\nIt hosts a csv-file called ",(0,r.jsx)(e.em,{children:"airports.csv"})," which contains what you need."]}),"\n",(0,r.jsx)(e.admonition,{type:"info",children:(0,r.jsxs)(e.p,{children:["Notice that this CSV-File is exactly what was downloaded in the previous step into\nthe folder ",(0,r.jsx)(e.em,{children:"data/stg_airports/result.csv"}),"."]})}),"\n",(0,r.jsx)(e.h2,{id:"next-step",children:"Next step"}),"\n",(0,r.jsxs)(e.p,{children:["Now that we know our input data, we can start our analysis.\nIn the next step, we will start ",(0,r.jsx)(e.em,{children:"Part 1 of the Getting Started Guide"}),"\nto do our first steps with Smart Data Lake Builder."]})]})}function p(t={}){const{wrapper:e}={...(0,i.R)(),...t.components};return e?(0,r.jsx)(e,{...t,children:(0,r.jsx)(h,{...t})}):h(t)}},8453:(t,e,n)=>{n.d(e,{R:()=>s,x:()=>o});var r=n(6540);const i={},a=r.createContext(i);function s(t){const e=r.useContext(a);return r.useMemo((function(){return"function"==typeof t?t(e):{...e,...t}}),[e,t])}function o(t){let e;return e=t.disableParentContext?"function"==typeof t.components?t.components(i):t.components||i:s(t.components),r.createElement(a.Provider,{value:e},t.children)}}}]);