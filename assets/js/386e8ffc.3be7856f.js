"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[4394],{3905:(e,t,r)=>{r.d(t,{Zo:()=>d,kt:()=>k});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},d=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,d=o(e,["components","mdxType","originalType","parentName"]),p=c(r),m=a,k=p["".concat(s,".").concat(m)]||p[m]||u[m]||i;return r?n.createElement(k,l(l({ref:t},d),{},{components:r})):n.createElement(k,l({ref:t},d))}));function k(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,l=new Array(i);l[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[p]="string"==typeof e?e:a,l[1]=o;for(var c=2;c<i;c++)l[c]=r[c];return n.createElement.apply(null,l)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},251:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>u,frontMatter:()=>i,metadata:()=>o,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const i={id:"hoconSecrets",title:"Hocon Secrets"},l=void 0,o={unversionedId:"reference/hoconSecrets",id:"reference/hoconSecrets",title:"Hocon Secrets",description:"Usernames, passwords and other secrets should not be stored in your configuration files in clear text as these files are often stored directly in the version control system.",source:"@site/docs/reference/hoconSecrets.md",sourceDirName:"reference",slug:"/reference/hoconSecrets",permalink:"/docs/reference/hoconSecrets",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/hoconSecrets.md",tags:[],version:"current",frontMatter:{id:"hoconSecrets",title:"Hocon Secrets"},sidebar:"docs",previous:{title:"Hocon Variables",permalink:"/docs/reference/hoconVariables"},next:{title:"DAG",permalink:"/docs/reference/dag"}},s={},c=[{value:"Secret Providers (since version 2.5.0)",id:"secret-providers-since-version-250",level:2},{value:"Legacy Secret Providers (deprecated in version 2.5.0)",id:"legacy-secret-providers-deprecated-in-version-250",level:2},{value:"Secret Provider Configuration",id:"secret-provider-configuration",level:2},{value:"Azure KeyVault configuration",id:"azure-keyvault-configuration",level:4},{value:"Databricks Secret configuration",id:"databricks-secret-configuration",level:4},{value:"Custom Secret Provider",id:"custom-secret-provider",level:2}],d={toc:c},p="wrapper";function u(e){let{components:t,...i}=e;return(0,a.kt)(p,(0,n.Z)({},d,i,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"Usernames, passwords and other secrets should ",(0,a.kt)("em",{parentName:"p"},"not")," be stored in your configuration files ",(0,a.kt)("em",{parentName:"p"},"in clear text")," as these files are often stored directly in the version control system.\nThey should also not be visible in logfiles after execution. Secret providers are the secure solutions to work with credentials. "),(0,a.kt)("h2",{id:"secret-providers-since-version-250"},"Secret Providers (since version 2.5.0)"),(0,a.kt)("p",null,"In version 2.5.0 of SDL, new properties with an improved secret syntax were introduced.\nBased on this new syntax, SDL can determine if a secret provider should be used or if the value can\ndirectly be read from the configuration, either as plaintext or from an environment variable.\nThe actual secret can be selected using the format ",(0,a.kt)("inlineCode",{parentName:"p"},"###<SecretProviderId>#<SecretName>###"),"."),(0,a.kt)("p",null,"In the following snippet, the secrets are retrieved from a secret provider"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'user = "###AZKV#JDBCuser###"\npassword = "###AZKV#JDBCpw###"\n')),(0,a.kt)("p",null,"while "),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'user = "username"\npassword = "password123"\n')),(0,a.kt)("p",null,"provides plaintext secret values. To read from environment variables, write"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'user = "${JDBCuser}"\npassword = "${JDBCpw}"\n')),(0,a.kt)("p",null,"as explained in ",(0,a.kt)("a",{parentName:"p",href:"hoconVariables#environment-variables"},"Environment variables"),"."),(0,a.kt)("p",null,"Whether secret providers can be used for a property can be seen in its description in the\n",(0,a.kt)("a",{parentName:"p",href:"http://smartdatalake.ch/json-schema-viewer/index.html"},"Schema Viewer"),".\nFor example:"),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"SECRET",src:r(2688).Z,width:"1500",height:"595"})),(0,a.kt)("p",null,"Implemented secret providers are:"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Provider"),(0,a.kt)("th",{parentName:"tr",align:null},"Pattern"),(0,a.kt)("th",{parentName:"tr",align:null},"Meaning"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"file"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"###FILE#filename;secretName###")),(0,a.kt)("td",{parentName:"tr",align:null},"read ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName")," from file ",(0,a.kt)("inlineCode",{parentName:"td"},"filename"),". The file needs to be a property file with line format ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName=secretValue"))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"###AZKV#secretName###")),(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". KeyValut specified in global section (see below)")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Databricks Secret"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"###DBSECRET#secretName###")),(0,a.kt)("td",{parentName:"tr",align:null},"Databricks element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". Secret provider specified in global section (see below)")))),(0,a.kt)("p",null,"Additionaly, custom providers can be implemented, see ",(0,a.kt)("a",{parentName:"p",href:"#custom-secret-provider"},"Custom Secret Providers"),"."),(0,a.kt)("h2",{id:"legacy-secret-providers-deprecated-in-version-250"},"Legacy Secret Providers (deprecated in version 2.5.0)"),(0,a.kt)("p",null,"All configuration items ending with ",(0,a.kt)("inlineCode",{parentName:"p"},"...Variable"),", like BasicAuthMode's ",(0,a.kt)("inlineCode",{parentName:"p"},"userVariable")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"passwordVariable")," can be used with secret providers.\nThe actual secret can be selected using the format: ",(0,a.kt)("inlineCode",{parentName:"p"},"<SecretProviderId>#<SecretName>"),", e.g."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'userVariable = "AZKV#JDBCuser"\npasswordVariable = "AZKV#JDBCpw"\n')),(0,a.kt)("p",null,"Implemented secret providers are:"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Provider"),(0,a.kt)("th",{parentName:"tr",align:null},"Pattern"),(0,a.kt)("th",{parentName:"tr",align:null},"Meaning"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"clear text"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"CLEAR#pd")),(0,a.kt)("td",{parentName:"tr",align:null},"The secret will be used literally (cleartext). This is only recommended for test environments.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"environment variable"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"ENV#pd")),(0,a.kt)("td",{parentName:"tr",align:null},'The value for this secret will be read from the environment variable called "pd".')),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"file"),(0,a.kt)("td",{parentName:"tr",align:null},(0,a.kt)("inlineCode",{parentName:"td"},"FILE#filename;secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"read ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName")," from file ",(0,a.kt)("inlineCode",{parentName:"td"},"filename"),". The file needs to be a property file with line format ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName=secretValue"))),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"AZKV#secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"Azure KeyVault element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". KeyValut specified in global section (see below)")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Databricks Secret"),(0,a.kt)("td",{parentName:"tr",align:null},"e.g. ",(0,a.kt)("inlineCode",{parentName:"td"},"DBSECRET#secretName")),(0,a.kt)("td",{parentName:"tr",align:null},"Databricks element ",(0,a.kt)("inlineCode",{parentName:"td"},"secretName"),". Secret provider specified in global section (see below)")))),(0,a.kt)("h2",{id:"secret-provider-configuration"},"Secret Provider Configuration"),(0,a.kt)("h4",{id:"azure-keyvault-configuration"},"Azure KeyVault configuration"),(0,a.kt)("p",null,"An ",(0,a.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/azure/key-vault/general/"},"Azure KeyVault")," would be specified as follows, here using an ID ",(0,a.kt)("inlineCode",{parentName:"p"},"AZKV"),". The KeyVault name still needs to be specified."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global = {\n  secretProviders = {\n    AZKV = {\n      className: io.smartdatalake.util.azure.AzureKeyVaultSecretProvider\n      options = {\n        keyVaultName: <azure-key-vault-name>\n      }\n    }\n  }\n }\n")),(0,a.kt)("h4",{id:"databricks-secret-configuration"},"Databricks Secret configuration"),(0,a.kt)("p",null,"Using Databricks credentials and other values can be stored encrypted as ",(0,a.kt)("a",{parentName:"p",href:"https://docs.databricks.com/security/secrets/index.html"},"Databricks secrets"),". This can be used within SDLB by utilizing the class DatabricksSecretProvider. Here the ID ",(0,a.kt)("inlineCode",{parentName:"p"},"DBSECRETS")," is used for further SDLB configuration references. Further, the ",(0,a.kt)("a",{parentName:"p",href:"https://docs.databricks.com/security/secrets/secret-scopes.html"},"secret scope")," (here ",(0,a.kt)("inlineCode",{parentName:"p"},"scope=test"),") is required in Databricks. The configuration would look like:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global {\n  secretProviders {\n    DBSECRETS = {\n     className = io.smartdatalake.util.secrets.DatabricksSecretProvider\n     options = { scope = test }\n    }\n  }\n}\n")),(0,a.kt)("h2",{id:"custom-secret-provider"},"Custom Secret Provider"),(0,a.kt)("p",null,"Furthermore, a custom secret provider could be implemented as class using the trait SecretProvider and a constructor with parameter ",(0,a.kt)("inlineCode",{parentName:"p"},"options: Map[String,String]"),".\nThe specification in the global section would in general look like:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"global {\n  secretProviders {\n    <secretProviderId> = {\n     className = <fully qualified class name of SecretProvider>\n     options = { <options as key/value> }\n    }\n  }\n}\n")))}u.isMDXComponent=!0},2688:(e,t,r)=>{r.d(t,{Z:()=>n});const n=r.p+"assets/images/secrets_schema_viewer-f95a2607dddc6bf533576289b13f938b.png"}}]);