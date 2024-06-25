"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[1778],{6325:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>l,default:()=>p,frontMatter:()=>i,metadata:()=>c,toc:()=>u});var a=n(4848),o=n(8453),s=n(1470),r=n(9365);const i={title:"Compute Distances"},l=void 0,c={id:"getting-started/part-1/compute-distances",title:"Compute Distances",description:"Goal",source:"@site/docs/getting-started/part-1/compute-distances.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/compute-distances",permalink:"/docs/getting-started/part-1/compute-distances",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/compute-distances.md",tags:[],version:"current",frontMatter:{title:"Compute Distances"},sidebar:"tutorialSidebar",previous:{title:"Get Departure Coordinates",permalink:"/docs/getting-started/part-1/joining-departures-and-arrivals"},next:{title:"Industrializing our data pipeline",permalink:"/docs/getting-started/part-2/industrializing"}},d={},u=[{value:"Goal",id:"goal",level:2},{value:"Define output object",id:"define-output-object",level:2},{value:"Define compute_distances action",id:"define-compute_distances-action",level:2},{value:"Try it out",id:"try-it-out",level:2},{value:"The Execution DAG of the compute feed",id:"the-execution-dag-of-the-compute-feed",level:3},{value:"The Execution DAG of the .* feed",id:"the-execution-dag-of-the--feed",level:3},{value:"The Execution DAG of the failed .* feed",id:"the-execution-dag-of-the-failed--feed",level:3}];function h(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",h3:"h3",p:"p",pre:"pre",strong:"strong",...(0,o.R)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsx)(t.p,{children:"In this part, we will compute the distances between departure and arrival airports\nso that our railway enthusiast Tom can see which planes could be replaced by rail traffic."}),"\n",(0,a.jsx)(t.h2,{id:"define-output-object",children:"Define output object"}),"\n",(0,a.jsx)(t.p,{children:"Let's define our final output object for this part:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  btl-distances {\n    type = CsvFileDataObject\n    path = "~{id}"\n  }\n'})}),"\n",(0,a.jsx)(t.h2,{id:"define-compute_distances-action",children:"Define compute_distances action"}),"\n",(0,a.jsxs)(t.p,{children:["How do we compute the distances ?\nThe answer is in the file ",(0,a.jsx)(t.em,{children:"src/main/scala/com/sample/ComputeDistanceTransformer.scala"}),"."]}),"\n",(0,a.jsx)(t.p,{children:"So far, we only used SQL transformers.\nBut more complex transformations can be written in custom code and referenced in the config.\nThis gives you great flexibility for cases with specific business logic, such as this one."}),"\n",(0,a.jsxs)(t.p,{children:["We have one input and one output: therefore our custom class is a ",(0,a.jsx)(t.em,{children:"CustomDfTransformer"})," (instead of CustomDf",(0,a.jsx)(t.strong,{children:"s"}),"Transformer).\nIt takes the input dataframe ",(0,a.jsx)(t.em,{children:"df"})," and calls a User Defined Function called ",(0,a.jsx)(t.em,{children:"calculateDistanceInKilometerUdf"}),"\nto do the computation.\nIt expects the column names dep_latitude_deg, dep_longitude_deg, arr_latitude_deg and arr_longitude_deg in the input.\nThis matches the column names we used in the SQL-Code in the last-step."]}),"\n",(0,a.jsxs)(t.p,{children:["We won't go into the details of the Udf.\nYou can follow this ",(0,a.jsx)(t.a,{href:"https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula",children:"stackoverflow link"}),"\nif you want to learn more."]}),"\n",(0,a.jsxs)(t.p,{children:["Finally, the transformation writes the result into a column called distance.\nIt also adds a column ",(0,a.jsx)(t.em,{children:"could_be_done_by_rail"})," to the output that simply checks if the distance is smaller than 500 km."]}),"\n",(0,a.jsx)(t.p,{children:"In order to wire this CustomTransformation into our config, we add the following action:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"  compute-distances {\n    type = CopyAction\n    inputId = btl-departures-arrivals-airports\n    outputId = btl-distances\n    transformers = [{\n      type = ScalaClassSparkDfTransformer\n      className = com.sample.ComputeDistanceTransformer\n    }]\n    metadata {\n      feed = compute\n    }\n  }\n"})}),"\n",(0,a.jsxs)(t.p,{children:["We used a CopyAction and told it to execute the code in the class ",(0,a.jsx)(t.em,{children:"com.sample.ComputeDistanceTransformer"})," to transform the data.\nWe could also have used a CustomDataFrameAction like in the previous step,\nbut this would have resulted in more complex code working with lists of inputs, outputs and transformers."]}),"\n",(0,a.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,a.jsxs)(t.p,{children:[(0,a.jsx)(t.a,{target:"_blank",href:n(463).A+"",children:"This"})," is how the final config-file looks like."]}),"\n",(0,a.jsxs)(t.p,{children:["To use the Java Code in our sdl-spark docker image, we have to compile it.\nYou have already done this in the ",(0,a.jsx)(t.a,{href:"/docs/getting-started/setup",children:"setup"}),", but lets review this step again. It can be done by using a maven docker image as follows"]}),"\n",(0,a.jsxs)(s.A,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,a.jsx)(r.A,{value:"docker",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:'mkdir .mvnrepo\ndocker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'})})}),(0,a.jsx)(r.A,{value:"podman",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'})})})]}),"\n",(0,a.jsx)(t.p,{children:"or you can also use maven directly if you have Java SDK and Maven installed"}),"\n",(0,a.jsx)(t.p,{children:"mvn package"}),"\n",(0,a.jsxs)(t.p,{children:["This creates a jar-file ./target/getting-started-1.0.jar containing the compiled Scala classes.\nThe ",(0,a.jsx)(t.em,{children:"docker run"})," includes a parameter to mount ./target into the docker image, which makes this jar-file accessible to SDL."]}),"\n",(0,a.jsx)(t.p,{children:"Now you can start SDL again:"}),"\n",(0,a.jsxs)(s.A,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,a.jsx)(r.A,{value:"docker",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel compute\n"})})}),(0,a.jsx)(r.A,{value:"podman",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel compute\n"})})})]}),"\n",(0,a.jsxs)(t.p,{children:["Under ",(0,a.jsx)(t.em,{children:"data/btl-distances"})," you can now see the final result."]}),"\n",(0,a.jsx)(t.h3,{id:"the-execution-dag-of-the-compute-feed",children:"The Execution DAG of the compute feed"}),"\n",(0,a.jsx)(t.p,{children:"In the console, you probably started noticing some pretty ASCII Art that looks like this:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"                         \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                         \u2502start\u2502\n                         \u2514\u252c\u2500\u2500\u252c\u2500\u2518\n                          \u2502  \u2502\n                          \u2502  \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                          v                      \u2502\n     \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510 \u2502\n     \u2502select-airport-cols SUCCEEDED PT4.421793S\u2502 \u2502\n     \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518 \u2502\n                     \u2502                           \u2502\n                     v                           v\n     \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n     \u2502join_departures_airports SUCCEEDED PT2.938995S\u2502\n     \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n                            \u2502\n                            v\n       \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n       \u2502compute_distances SUCCEEDED PT1.160045S\u2502\n       \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n"})}),"\n",(0,a.jsxs)(t.p,{children:["This is the Execution ",(0,a.jsx)(t.em,{children:"DAG"})," of our data pipeline.\nSDL internally builds a Directed Acyclic Graph (DAG) to analyze all dependencies between your actions.\nBecause it's acyclic, it cannot have loops, so it's impossible to define an action that depends on an output of subsequent actions.\nSDL uses this DAG to optimize execution of your pipeline as it knows in which order the pipeline needs to execute."]}),"\n",(0,a.jsx)(t.p,{children:"What you see in the logs, is a representation of what the SDL has determined as dependencies.\nIf you don't get the results you expect, it's good to check if the DAG looks correct.\nAt the end you will also see the graph again with status indicators (SUCCEEDED, CANCELLED, ...) and the duration it took to execute the action."}),"\n",(0,a.jsx)(t.admonition,{title:"Hold on",type:"tip",children:(0,a.jsxs)(t.p,{children:["It's worth pausing at this point to appreciate this fact:",(0,a.jsx)(t.br,{}),"\n","You didn't have to explicitly tell SDL how to execute your actions.\nYou also didn't have to define the dependencies between your actions.\nJust from the definitions of DataObjects and Actions alone, SDL builds a DAG and knows what needs to be executed and how."]})}),"\n",(0,a.jsx)(t.h3,{id:"the-execution-dag-of-the--feed",children:"The Execution DAG of the .* feed"}),"\n",(0,a.jsx)(t.p,{children:"You can also execute the entire data pipeline by selecting all feeds:"}),"\n",(0,a.jsxs)(s.A,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,a.jsx)(r.A,{value:"docker",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel '.*'\n"})})}),(0,a.jsx)(r.A,{value:"podman",children:(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-jsx",children:"podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel '.*'\n"})})})]}),"\n",(0,a.jsx)(t.p,{children:"The successful execution DAG looks like this"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"                                            \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                                            \u2502start\u2502\n                                            \u2514\u2500\u252c\u2500\u252c\u2500\u2518\n                                              \u2502 \u2502\n                                              \u2502 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                                              v                       \u2502\n                   \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510          \u2502\n                   \u2502download-airports SUCCEEDED PT8.662886S\u2502          \u2502\n                   \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518          \u2502\n                          \u2502                                           \u2502\n                          v                                           v\n     \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510 \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n     \u2502select-airport-cols SUCCEEDED PT4.629163S\u2502 \u2502download-departures SUCCEEDED PT0.564972S\u2502\n     \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518                                      \n                                        v               v\n                        \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                        \u2502join_departures_airports SUCCEEDED PT4.453514S\u2502\n                        \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n                                               \u2502\n                                               v\n                           \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                           \u2502compute_distances SUCCEEDED PT1.404391S\u2502\n                           \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n"})}),"\n",(0,a.jsx)(t.p,{children:"SDL was able to determine that download-airports and download-departures can be executed in parallel,\nindependently from each other. It's only at join-departures-airports and beyond that both Data Sources are needed."}),"\n",(0,a.jsx)(t.p,{children:"SDL has command-line option called --parallelism which allows you to set the degree of parallelism used when executing the DAG run.\nPer default, it's set to 1."}),"\n",(0,a.jsx)(t.h3,{id:"the-execution-dag-of-the-failed--feed",children:"The Execution DAG of the failed .* feed"}),"\n",(0,a.jsxs)(t.p,{children:["If you set the option ",(0,a.jsx)(t.em,{children:"readTimeoutMs=0"})," in the DataObject ",(0,a.jsx)(t.em,{children:"ext-departures"}),"  it's possible that the ",(0,a.jsx)(t.em,{children:"download-departures"})," action fails.\nThat's because SDL abandons the download because the REST-Service is too slow to respond.\nIf that happens, the DAG will look like this:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"    21/09/14 09:05:36 ERROR ActionDAGRun: Exec: TaskFailedException: Task download-departures failed. Root cause is 'WebserviceException: connect timed out': WebserviceException: connect timed out\n    21/09/14 09:05:36 INFO ActionDAGRun$: Exec FAILED for .* runId=1 attemptId=1:\n                                           \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                                           \u2502start\u2502\n                                           \u2514\u2500\u252c\u2500\u252c\u2500\u2518\n                                             \u2502 \u2502\n                                             \u2502 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                                             v                       \u2502\n                  \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510          \u2502\n                  \u2502download-airports SUCCEEDED PT6.638844S\u2502          \u2502\n                  \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518          \u2502\n                          \u2502                                          \u2502\n                          v                                          v\n     \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510 \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n     \u2502select-airport-cols SUCCEEDED PT3.064118S\u2502 \u2502download-departures FAILED PT1.084153S\u2502\n     \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2518 \u2514\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n                                        \u2502           \u2502\n                                        v           v\n                            \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                            \u2502join-departures-airports CANCELLED\u2502\n                            \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n                                              \u2502\n                                              v\n                                \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                                \u2502compute-distances CANCELLED\u2502\n                                \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n"})}),"\n",(0,a.jsxs)(t.p,{children:["As you can see, in this example, the action ",(0,a.jsx)(t.em,{children:"download-departures"})," failed because of a timeout.\nBecause SDL determined that both downloads are independent, it was able to complete ",(0,a.jsx)(t.em,{children:"select-airport-cols"})," succesfully."]}),"\n",(0,a.jsxs)(t.p,{children:["As explained in ",(0,a.jsx)(t.a,{href:"/docs/getting-started/part-1/select-columns#more-on-feeds",children:"a previous step of the guide"}),",\nif your download fails you will have to re-execute the download feed before being able to execute\n.* again."]}),"\n",(0,a.jsxs)(t.p,{children:[(0,a.jsx)(t.strong,{children:"Congratulations!"}),(0,a.jsx)(t.br,{}),"\n","You successfully recreated the configuration file that is contained in the Docker Image you ran in the first step."]})]})}function p(e={}){const{wrapper:t}={...(0,o.R)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(h,{...e})}):h(e)}},9365:(e,t,n)=>{n.d(t,{A:()=>r});n(6540);var a=n(53);const o={tabItem:"tabItem_Ymn6"};var s=n(4848);function r(e){let{children:t,hidden:n,className:r}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,a.A)(o.tabItem,r),hidden:n,children:t})}},1470:(e,t,n)=>{n.d(t,{A:()=>w});var a=n(6540),o=n(53),s=n(3104),r=n(6347),i=n(205),l=n(7485),c=n(1682),d=n(9466);function u(e){return a.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function h(e){const{values:t,children:n}=e;return(0,a.useMemo)((()=>{const e=t??function(e){return u(e).map((e=>{let{props:{value:t,label:n,attributes:a,default:o}}=e;return{value:t,label:n,attributes:a,default:o}}))}(n);return function(e){const t=(0,c.X)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function p(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function m(e){let{queryString:t=!1,groupId:n}=e;const o=(0,r.W6)(),s=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,l.aZ)(s),(0,a.useCallback)((e=>{if(!s)return;const t=new URLSearchParams(o.location.search);t.set(s,e),o.replace({...o.location,search:t.toString()})}),[s,o])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:o}=e,s=h(e),[r,l]=(0,a.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=n.find((e=>e.default))??n[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:s}))),[c,u]=m({queryString:n,groupId:o}),[f,x]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[o,s]=(0,d.Dv)(n);return[o,(0,a.useCallback)((e=>{n&&s.set(e)}),[n,s])]}({groupId:o}),v=(()=>{const e=c??f;return p({value:e,tabValues:s})?e:null})();(0,i.A)((()=>{v&&l(v)}),[v]);return{selectedValue:r,selectValue:(0,a.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error(`Can't select invalid tab value=${e}`);l(e),u(e),x(e)}),[u,x,s]),tabValues:s}}var x=n(2303);const v={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var g=n(4848);function b(e){let{className:t,block:n,selectedValue:a,selectValue:r,tabValues:i}=e;const l=[],{blockElementScrollPositionUntilNextRender:c}=(0,s.a_)(),d=e=>{const t=e.currentTarget,n=l.indexOf(t),o=i[n].value;o!==a&&(c(t),r(o))},u=e=>{let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{const n=l.indexOf(e.currentTarget)+1;t=l[n]??l[0];break}case"ArrowLeft":{const n=l.indexOf(e.currentTarget)-1;t=l[n]??l[l.length-1];break}}t?.focus()};return(0,g.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.A)("tabs",{"tabs--block":n},t),children:i.map((e=>{let{value:t,label:n,attributes:s}=e;return(0,g.jsx)("li",{role:"tab",tabIndex:a===t?0:-1,"aria-selected":a===t,ref:e=>l.push(e),onKeyDown:u,onClick:d,...s,className:(0,o.A)("tabs__item",v.tabItem,s?.className,{"tabs__item--active":a===t}),children:n??t},t)}))})}function j(e){let{lazy:t,children:n,selectedValue:o}=e;const s=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=s.find((e=>e.props.value===o));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return(0,g.jsx)("div",{className:"margin-top--md",children:s.map(((e,t)=>(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==o})))})}function D(e){const t=f(e);return(0,g.jsxs)("div",{className:(0,o.A)("tabs-container",v.tabList),children:[(0,g.jsx)(b,{...e,...t}),(0,g.jsx)(j,{...e,...t})]})}function w(e){const t=(0,x.A)();return(0,g.jsx)(D,{...e,children:u(e.children)},String(t))}},463:(e,t,n)=>{n.d(t,{A:()=>a});const a=n.p+"assets/files/application-part1-compute-final-5b480d6fb0f3272916beb958014529c9.conf"},8453:(e,t,n)=>{n.d(t,{R:()=>r,x:()=>i});var a=n(6540);const o={},s=a.createContext(o);function r(e){const t=a.useContext(s);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:r(e.components),a.createElement(s.Provider,{value:t},e.children)}}}]);