"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[109],{3905:function(t,e,n){n.d(e,{Zo:function(){return u},kt:function(){return m}});var r=n(7294);function a(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function o(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function i(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?o(Object(n),!0).forEach((function(e){a(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function s(t,e){if(null==t)return{};var n,r,a=function(t,e){if(null==t)return{};var n,r,a={},o=Object.keys(t);for(r=0;r<o.length;r++)n=o[r],e.indexOf(n)>=0||(a[n]=t[n]);return a}(t,e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(t);for(r=0;r<o.length;r++)n=o[r],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(a[n]=t[n])}return a}var l=r.createContext({}),p=function(t){var e=r.useContext(l),n=e;return t&&(n="function"==typeof t?t(e):i(i({},e),t)),n},u=function(t){var e=p(t.components);return r.createElement(l.Provider,{value:e},t.children)},c={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},d=r.forwardRef((function(t,e){var n=t.components,a=t.mdxType,o=t.originalType,l=t.parentName,u=s(t,["components","mdxType","originalType","parentName"]),d=p(n),m=a,h=d["".concat(l,".").concat(m)]||d[m]||c[m]||o;return n?r.createElement(h,i(i({ref:e},u),{},{components:n})):r.createElement(h,i({ref:e},u))}));function m(t,e){var n=arguments,a=e&&e.mdxType;if("string"==typeof t||a){var o=n.length,i=new Array(o);i[0]=d;var s={};for(var l in e)hasOwnProperty.call(e,l)&&(s[l]=e[l]);s.originalType=t,s.mdxType="string"==typeof t?t:a,i[1]=s;for(var p=2;p<o;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},1385:function(t,e,n){n.r(e),n.d(e,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return p},toc:function(){return u},default:function(){return d}});var r=n(7462),a=n(3366),o=(n(7294),n(3905)),i=["components"],s={title:"Joining It Together"},l=void 0,p={unversionedId:"getting-started/part-1/joining-it-together",id:"getting-started/part-1/joining-it-together",title:"Joining It Together",description:"Goal",source:"@site/docs/getting-started/part-1/joining-it-together.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/joining-it-together",permalink:"/docs/getting-started/part-1/joining-it-together",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/joining-it-together.md",tags:[],version:"current",frontMatter:{title:"Joining It Together"},sidebar:"docs",previous:{title:"Select Columns",permalink:"/docs/getting-started/part-1/select-columns"},next:{title:"Get Departure Coordinates",permalink:"/docs/getting-started/part-1/joining-departures-and-arrivals"}},u=[{value:"Goal",id:"goal",children:[],level:2},{value:"Define output object",id:"define-output-object",children:[],level:2},{value:"Define join_departures_airports action",id:"define-join_departures_airports-action",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2}],c={toc:u};function d(t){var e=t.components,s=(0,a.Z)(t,i);return(0,o.kt)("wrapper",(0,r.Z)({},c,s,{components:e,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"So now we have data from departures in our stage layer, and we have cleaned data for airports in our integration layer.\nIn this step we will finally join both data sources together.\nWe will continue based on the config file available ",(0,o.kt)("a",{target:"_blank",href:n(4585).Z},"here"),".\nAt the end of the step, we will have all planes departing from Bern Airport\nin the given timeframe along with their readable destination airport names, as well as geo-coordinates."),(0,o.kt)("p",null,"Like in the previous step, we need one more action and one DataObject for our output."),(0,o.kt)("h2",{id:"define-output-object"},"Define output object"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  btl-connected-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n  }\n')),(0,o.kt)("h2",{id:"define-join_departures_airports-action"},"Define join_departures_airports action"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  join-departures-airports {\n    type = CustomSparkAction\n    inputIds = [stg-departures, int-airports]\n    outputIds = [btl-connected-airports]\n    transformers = [{\n      type = SQLDfsTransformer\n      code = {\n        btl-connected-airports = """select stg_departures.estdepartureairport, stg_departures.estarrivalairport,\n        airports.*\n         from stg_departures join int_airports airports on stg_departures.estArrivalAirport = airports.ident"""\n      }\n    }\n    ]\n    metadata {\n      feed = compute\n    }\n  }\n')),(0,o.kt)("p",null,"Now it gets interesting, a couple of things to note here:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"This time, we changed the Action Type from CopyAction to CustomSparkAction.\nUse CustomSparkAction when you need to do complex operations. For instance, CustomSparkAction allows multiple inputs,\nwhich CopyAction does not."),(0,o.kt)("li",{parentName:"ul"},"Our input/output fields are now called inputId",(0,o.kt)("strong",{parentName:"li"},"s")," and outputId",(0,o.kt)("strong",{parentName:"li"},"s")," and they take a list of DataObject ids.\nSimilarly, our transformer is now of type SQLDf",(0,o.kt)("strong",{parentName:"li"},"s"),"Transformer.\nAgain, the ",(0,o.kt)("strong",{parentName:"li"},"s")," is important, since it shows that multiple inputs/output Data Objects are possible, which is what we need in this step.\nIn the previous step, we defined a SQLDfTransformer because we only needed one input."),(0,o.kt)("li",{parentName:"ul"},"Finally, the ",(0,o.kt)("em",{parentName:"li"},"SQLDfsTransformer")," expects it's code as a HOCON object rather than as a string.\nThis is due to the fact that you could have multiple\noutputs, in which case you would need to name them in order to distinguish them.\nIn our case, there is only one output DataObject: ",(0,o.kt)("em",{parentName:"li"},"btl-connected-airports"),".\nThe SQL-Code itself is just a join between the two input Data Objects on the ICAO identifier.\nNote that we can just select all columns from airports, since we selected the ones that interest us in the previous step.")),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"Tip: Use only one output")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"As you can see, with CustomSparkAction it's possible to read from multiple inputs and write to multiple outputs.\nWe usually discourage writing to multiple Data Objects in one action though.\nAt some point, you will want to use the metadata from SDL to analyze your data lineage. If you have a CustomSparkAction\nwith multiple inputs and multiple outputs (an M:N-relationship), SDL assumes that all outputs depend on all inputs. This might add\nsome dependencies between DataObjects that don't really exist in the CustomSparkAction.\nAlways using one Data Object as output will make your data lineage more detailed and clear."))),(0,o.kt)("h2",{id:"try-it-out"},"Try it out"),(0,o.kt)("p",null,"You can run the usual ",(0,o.kt)("em",{parentName:"p"},"docker run")," command:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/config:/mnt/config demo:latest -c /mnt/config --feed-sel compute\n")),(0,o.kt)("p",null,"You should now see the resulting files in ",(0,o.kt)("em",{parentName:"p"},"data/btl-connected-airports"),".\nGreat! Now we have names and coordinates of destination airports.\nWe are just missing the coordinates of Bern Airport.\nLet's add them in the next step."))}d.isMDXComponent=!0},4585:function(t,e,n){e.Z=n.p+"assets/files/application-compute-part1-cols-92b2f882d2243742b48eba906672a859.conf"}}]);