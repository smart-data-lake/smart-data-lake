"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[4394],{3905:(e,t,r)=>{r.d(t,{Zo:()=>d,kt:()=>k});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},d=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),p=c(r),m=a,k=p["".concat(s,".").concat(m)]||p[m]||u[m]||i;return r?n.createElement(k,o(o({ref:t},d),{},{components:r})):n.createElement(k,o({ref:t},d))}));function k(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[p]="string"==typeof e?e:a,o[1]=l;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},251:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>l,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const i={id:"hoconSecrets",title:"Hocon Secrets"},o=void 0,l={unversionedId:"reference/hoconSecrets",id:"reference/hoconSecrets",title:"Hocon Secrets",description:"Secret User and Password Variables",source:"@site/docs/reference/hoconSecrets.md",sourceDirName:"reference",slug:"/reference/hoconSecrets",permalink:"/docs/reference/hoconSecrets",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/hoconSecrets.md",tags:[],version:"current",frontMatter:{id:"hoconSecrets",title:"Hocon Secrets"},sidebar:"docs",previous:{title:"Hocon Configurations",permalink:"/docs/reference/hoconOverview"},next:{title:"Hocon Variables",permalink:"/docs/reference/hoconVariables"}},s={},c=[{value:"Secret User and Password Variables",id:"secret-user-and-password-variables",level:2},{value:"Secret Providers",id:"secret-providers",level:2},{value:"Azure KeyVault configuration",id:"azure-keyvault-configuration",level:4},{value:"Databricks Secret configuration",id:"databricks-secret-configuration",level:4},{value:"Custom Secret Provider",id:"custom-secret-provider",level:2}],d={toc:c},p="wrapper";function u(e){let{components:t,...r}=e;return(0,a.kt)(p,(0,n.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"secret-user-and-password-variables"},"Secret User and Password Variables"),(0,a.kt)("p",null,"Usernames, passwords and other secrets should ",(0,a.kt)("em",{parentName:"p"},"not")," be stored in your configuration files ",(0,a.kt)("em",{parentName:"p"},"in clear text")," as these files are often stored directly in the version control system.\nThey should also not be visible in logfiles after execution."),(0,a.kt)("p",null,"Secret providers are the secure solutions to work with credentials.\nAll configuration items ending with ",(0,a.kt)("inlineCode",{parentName:"p"},"...Variable"),", like BasicAuthMode's ",(0,a.kt)("inlineCode",{parentName:"p"},"userVariable")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"passwordVariable")," can be used with secret providers.\nThe actual secret can be selected using the format: ",(0,a.kt)("inlineCode",{parentName:"p"},"<SecretProviderId>#<SecretName>"),", e.g."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'userVariable = "AZKV#JDBCuser"\npasswordVariable = "AZKV#JDBCpw"\n')),(0,a.kt)("h2",{id:"secret-providers"},"Secret Providers"),(0,a.kt)("p",null,"Implemented secret providers are:"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Provider"),(0,a.kt)("th",{parentName:"tr",align:null},"Pattern"),(0,a.kt)("th",{parentName:"tr",align:null},"Meaning"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"clear text"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"CLEAR#pd")),(0,a.kt)("td",{parentName:"tr",align:null},"The secret will be used literally (cleartext). This is only recommended for test environments.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"environment variable"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"ENV#pd")),(0,a.kt)("td",{parentName:"tr",align:null},'The value for this secret will be read from the environment variable called "pd".')),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"file"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"FILE#filename;secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"read ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName")," from file ",(0,a.kt)("inlineCode",{parentName:"td"},"filename"),". The file needs to be a property file with line format ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName=secretValue"))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"AZKV#secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". KeyValut specified in global section (see below)")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Databricks Secret"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"DBSECRET#secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"Databricks element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". Secret provider specified in global section (see below)")))),(0,a.kt)("p",null,"Additionaly, custom providers can be implemented, see ",(0,a.kt)("a",{parentName:"p",href:"#custom-secret-provider"},"Custom Secret Providers"),"."),(0,a.kt)("h4",{id:"azure-keyvault-configuration"},"Azure KeyVault configuration"),(0,a.kt)("p",null,"An ",(0,a.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/azure/key-vault/general/"},"Azure KeyVault")," would be specified as follows, here using an ID ",(0,a.kt)("inlineCode",{parentName:"p"},"AZKV"),". The KeyVault name still needs to be specified. "),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global = {\n  secretProviders = {\n    AZKV = {\n      className: io.smartdatalake.util.azure.AzureKeyVaultSecretProvider\n      options = {\n        keyVaultName: <azure-key-vault-name>\n      }\n    }\n  }\n }\n")),(0,a.kt)("h4",{id:"databricks-secret-configuration"},"Databricks Secret configuration"),(0,a.kt)("p",null,"Using Databricks credentials and other values can be stored encrypted as ",(0,a.kt)("a",{parentName:"p",href:"https://docs.databricks.com/security/secrets/index.html"},"Databricks secrets"),". This can be used within SDLB by utilizing the class DatabricksSecretProvider. Here the ID ",(0,a.kt)("inlineCode",{parentName:"p"},"DBSECRETS")," is used for further SDLB configuration references. Further, the ",(0,a.kt)("a",{parentName:"p",href:"https://docs.databricks.com/security/secrets/secret-scopes.html"},"secret scope")," (here ",(0,a.kt)("inlineCode",{parentName:"p"},"scope=test"),") is required in Databricks. The configuration would look like:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global {\n  secretProviders {\n    DBSECRETS = {\n     className = io.smartdatalake.util.secrets.DatabricksSecretProvider\n     options = { scope = test }\n    }\n  }\n}\n")),(0,a.kt)("h2",{id:"custom-secret-provider"},"Custom Secret Provider"),(0,a.kt)("p",null,"Furthermore, a custom secret provider could be implemented as class using the trait SecretProvider and a constructor with parameter ",(0,a.kt)("inlineCode",{parentName:"p"},"options: Map[String,String]"),".\nThe specification in the global section would in general look like:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global {\n  secretProviders {\n    <secretProviderId> = {\n     className = <fully qualified class name of SecretProvider>\n     options = { <options as key/value> }\n    }\n  }\n}\n")))}u.isMDXComponent=!0}}]);