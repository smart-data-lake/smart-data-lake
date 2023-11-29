"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3109],{6644:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>l,default:()=>h,frontMatter:()=>s,metadata:()=>u,toc:()=>d});var a=n(5893),r=n(1151),o=n(4866),i=n(5162);const s={title:"Joining It Together"},l=void 0,u={id:"getting-started/part-1/joining-it-together",title:"Joining It Together",description:"Goal",source:"@site/docs/getting-started/part-1/joining-it-together.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/joining-it-together",permalink:"/docs/getting-started/part-1/joining-it-together",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/joining-it-together.md",tags:[],version:"current",frontMatter:{title:"Joining It Together"},sidebar:"tutorialSidebar",previous:{title:"Select Columns",permalink:"/docs/getting-started/part-1/select-columns"},next:{title:"Get Departure Coordinates",permalink:"/docs/getting-started/part-1/joining-departures-and-arrivals"}},c={},d=[{value:"Goal",id:"goal",level:2},{value:"Define output object",id:"define-output-object",level:2},{value:"Define join_departures_airports action",id:"define-join_departures_airports-action",level:2},{value:"Try it out",id:"try-it-out",level:2}];function p(e){const t={a:"a",admonition:"admonition",code:"code",em:"em",h2:"h2",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,r.a)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsxs)(t.p,{children:["So now we have data from departures in our stage layer, and we have cleaned data for airports in our integration layer.\nIn this step we will finally join both data sources together.\nWe will continue based on the config file available ",(0,a.jsx)(t.a,{target:"_blank",href:n(5779).Z+"",children:"here"}),".\nAt the end of the step, we will have all planes departing from Bern Airport\nin the given timeframe along with their readable destination airport names, as well as geo-coordinates."]}),"\n",(0,a.jsx)(t.p,{children:"Like in the previous step, we need one more action and one DataObject for our output."}),"\n",(0,a.jsx)(t.h2,{id:"define-output-object",children:"Define output object"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  btl-connected-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n  }\n'})}),"\n",(0,a.jsx)(t.h2,{id:"define-join_departures_airports-action",children:"Define join_departures_airports action"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  join-departures-airports {\n    type = CustomDataFrameAction\n    inputIds = [stg-departures, int-airports]\n    outputIds = [btl-connected-airports]\n    transformers = [{\n      type = SQLDfsTransformer\n      code = {\n        btl-connected-airports = """select stg_departures.estdepartureairport, stg_departures.estarrivalairport,\n        airports.*\n          from stg_departures join int_airports airports on stg_departures.estArrivalAirport = airports.ident"""\n      }\n    }\n    ]\n    metadata {\n      feed = compute\n    }\n  }\n'})}),"\n",(0,a.jsx)(t.p,{children:"Now it gets interesting, a couple of things to note here:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsx)(t.li,{children:"This time, we changed the Action Type from CopyAction to CustomDataFrameAction.\nUse CustomDataFrameAction when you need to do complex operations. For instance, CustomDataFrameAction allows multiple inputs,\nwhich CopyAction does not."}),"\n",(0,a.jsxs)(t.li,{children:["Our input/output fields are now called inputId",(0,a.jsx)(t.strong,{children:"s"})," and outputId",(0,a.jsx)(t.strong,{children:"s"})," and they take a list of DataObject ids.\nSimilarly, our transformer is now of type SQLDf",(0,a.jsx)(t.strong,{children:"s"}),"Transformer.\nAgain, the ",(0,a.jsx)(t.strong,{children:"s"})," is important, since it shows that multiple inputs/output Data Objects are possible, which is what we need in this step.\nIn the previous step, we defined a SQLDfTransformer because we only needed one input."]}),"\n",(0,a.jsxs)(t.li,{children:["Finally, the ",(0,a.jsx)(t.em,{children:"SQLDfsTransformer"})," expects its code as a HOCON object rather than as a string.\nThis is due to the fact that you could have multiple\noutputs, in which case you would need to name them in order to distinguish them.\nIn our case, there is only one output DataObject: ",(0,a.jsx)(t.em,{children:"btl-connected-airports"}),".\nThe SQL-Code itself is just a join between the two input Data Objects on the ICAO identifier.\nNote that we can just select all columns from airports, since we selected the ones that interest us in the previous step."]}),"\n"]}),"\n",(0,a.jsx)(t.admonition,{title:"Tip: Use only one output",type:"tip",children:(0,a.jsxs)(t.p,{children:["As you can see, with CustomDataFrameAction it's possible to read from multiple inputs and write to multiple outputs.\nWe usually discourage writing to multiple Data Objects in one action though.\nAt some point, you will want to use the metadata from SDL to analyze your data lineage. If you have a CustomDataFrameAction\nwith multiple inputs and multiple outputs (an M",":N-relationship","), SDL assumes that all outputs depend on all inputs. This might add\nsome dependencies between DataObjects that don't really exist in the CustomDataFrameAction.\nAlways using one Data Object as output will make your data lineage more detailed and clear."]})}),"\n",(0,a.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,a.jsxs)(t.p,{children:["You can run the usual ",(0,a.jsx)(t.em,{children:"docker run"})," command:"]}),"\n",(0,a.jsxs)(o.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,a.jsx)(i.Z,{value:"docker",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel compute\n"})})}),(0,a.jsx)(i.Z,{value:"podman",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel compute\n"})})})]}),"\n",(0,a.jsxs)(t.p,{children:["You should now see the resulting files in ",(0,a.jsx)(t.em,{children:"data/btl-connected-airports"}),".\nGreat! Now we have names and coordinates of destination airports.\nWe are just missing the coordinates of Bern Airport.\nLet's add them in the next step."]})]})}function h(e={}){const{wrapper:t}={...(0,r.a)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(p,{...e})}):p(e)}},5162:(e,t,n)=>{n.d(t,{Z:()=>i});n(7294);var a=n(6010);const r={tabItem:"tabItem_Ymn6"};var o=n(5893);function i(e){let{children:t,hidden:n,className:i}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,a.Z)(r.tabItem,i),hidden:n,children:t})}},4866:(e,t,n)=>{n.d(t,{Z:()=>y});var a=n(7294),r=n(6010),o=n(2466),i=n(6550),s=n(469),l=n(1980),u=n(7392),c=n(12);function d(e){return a.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function p(e){const{values:t,children:n}=e;return(0,a.useMemo)((()=>{const e=t??function(e){return d(e).map((e=>{let{props:{value:t,label:n,attributes:a,default:r}}=e;return{value:t,label:n,attributes:a,default:r}}))}(n);return function(e){const t=(0,u.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function h(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function m(e){let{queryString:t=!1,groupId:n}=e;const r=(0,i.k6)(),o=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,l._X)(o),(0,a.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(r.location.search);t.set(o,e),r.replace({...r.location,search:t.toString()})}),[o,r])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:r}=e,o=p(e),[i,l]=(0,a.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!h({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=n.find((e=>e.default))??n[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:o}))),[u,d]=m({queryString:n,groupId:r}),[f,g]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[r,o]=(0,c.Nk)(n);return[r,(0,a.useCallback)((e=>{n&&o.set(e)}),[n,o])]}({groupId:r}),b=(()=>{const e=u??f;return h({value:e,tabValues:o})?e:null})();(0,s.Z)((()=>{b&&l(b)}),[b]);return{selectedValue:i,selectValue:(0,a.useCallback)((e=>{if(!h({value:e,tabValues:o}))throw new Error(`Can't select invalid tab value=${e}`);l(e),d(e),g(e)}),[d,g,o]),tabValues:o}}var g=n(2389);const b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var j=n(5893);function v(e){let{className:t,block:n,selectedValue:a,selectValue:i,tabValues:s}=e;const l=[],{blockElementScrollPositionUntilNextRender:u}=(0,o.o5)(),c=e=>{const t=e.currentTarget,n=l.indexOf(t),r=s[n].value;r!==a&&(u(t),i(r))},d=e=>{let t=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{const n=l.indexOf(e.currentTarget)+1;t=l[n]??l[0];break}case"ArrowLeft":{const n=l.indexOf(e.currentTarget)-1;t=l[n]??l[l.length-1];break}}t?.focus()};return(0,j.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":n},t),children:s.map((e=>{let{value:t,label:n,attributes:o}=e;return(0,j.jsx)("li",{role:"tab",tabIndex:a===t?0:-1,"aria-selected":a===t,ref:e=>l.push(e),onKeyDown:d,onClick:c,...o,className:(0,r.Z)("tabs__item",b.tabItem,o?.className,{"tabs__item--active":a===t}),children:n??t},t)}))})}function w(e){let{lazy:t,children:n,selectedValue:r}=e;const o=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===r));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return(0,j.jsx)("div",{className:"margin-top--md",children:o.map(((e,t)=>(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==r})))})}function x(e){const t=f(e);return(0,j.jsxs)("div",{className:(0,r.Z)("tabs-container",b.tabList),children:[(0,j.jsx)(v,{...e,...t}),(0,j.jsx)(w,{...e,...t})]})}function y(e){const t=(0,g.Z)();return(0,j.jsx)(x,{...e,children:d(e.children)},String(t))}},5779:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/application-part1-compute-cols-ed63adf7b5b9e4980a6a36d232cfdc91.conf"},1151:(e,t,n)=>{n.d(t,{Z:()=>s,a:()=>i});var a=n(7294);const r={},o=a.createContext(r);function i(e){const t=a.useContext(o);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function s(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),a.createElement(o.Provider,{value:t},e.children)}}}]);