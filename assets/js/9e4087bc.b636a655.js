"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2711],{9331:(e,a,t)=>{t.r(a),t.d(a,{default:()=>o});t(6540);var r=t(5489),s=t(1312),i=t(9024),l=t(8511),n=t(1107),c=t(4848);function d(e){let{year:a,posts:t}=e;return(0,c.jsxs)(c.Fragment,{children:[(0,c.jsx)(n.A,{as:"h3",id:a,children:a}),(0,c.jsx)("ul",{children:t.map((e=>(0,c.jsx)("li",{children:(0,c.jsxs)(r.A,{to:e.metadata.permalink,children:[e.metadata.formattedDate," - ",e.metadata.title]})},e.metadata.date)))})]})}function h(e){let{years:a}=e;return(0,c.jsx)("section",{className:"margin-vert--lg",children:(0,c.jsx)("div",{className:"container",children:(0,c.jsx)("div",{className:"row",children:a.map(((e,a)=>(0,c.jsx)("div",{className:"col col--4 margin-vert--lg",children:(0,c.jsx)(d,{...e})},a)))})})})}function o(e){let{archive:a}=e;const t=(0,s.T)({id:"theme.blog.archive.title",message:"Archive",description:"The page & hero title of the blog archive page"}),r=(0,s.T)({id:"theme.blog.archive.description",message:"Archive",description:"The page & hero description of the blog archive page"}),d=function(e){const a=e.reduce(((e,a)=>{const t=a.metadata.date.split("-")[0],r=e.get(t)??[];return e.set(t,[a,...r])}),new Map);return Array.from(a,(e=>{let[a,t]=e;return{year:a,posts:t}}))}(a.blogPosts);return(0,c.jsxs)(c.Fragment,{children:[(0,c.jsx)(i.be,{title:t,description:r}),(0,c.jsxs)(l.A,{children:[(0,c.jsx)("header",{className:"hero hero--primary",children:(0,c.jsxs)("div",{className:"container",children:[(0,c.jsx)(n.A,{as:"h1",className:"hero__title",children:t}),(0,c.jsx)("p",{className:"hero__subtitle",children:r})]})}),(0,c.jsx)("main",{children:d.length>0&&(0,c.jsx)(h,{years:d})})]})]})}}}]);