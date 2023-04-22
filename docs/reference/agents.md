---
id: agents
title: agents (Experimental)
---

NOTE
This feature is Experimental

The Agent feature allows to have a main instance of SDLB sending instructions to another Instance of SDLB, called the Remote Agent.
This Remote Agent then executes the action in its environment and returns the schema of the resulting data in an Empty SparkSubFeed.
Only metadata is sent over the connection between Remote Agent and Main Instance.
The Main Instance then fetches the result if needed using its own connection to the Data.

To use this feature, you must define a section called agents in your HOCON config.
This config section allows to describe how to reach the agents as well as the connections that the remote agents have.
TODO update schema viewer.

Example: Agents section defining a JettyAgent along with a private Connection that only the Agent can access.
```
agents {
  agent-dummy {
    type = JettyAgent
    url = "ws://localhost:4441/ws/"
    connections {
      remoteFile {
        id = remoteFile
        type = HadoopFileConnection
        pathPrefix = "target/jetty_agent_dummy_connection"
      }
    }
  }
}
```

Once you have defined your agents, you can reference them in your actions by specifying the field agentId.
In the example above the agentId would be agent-dummy.
If you define an agentId in your action, it will be executed on that agent.
More precisely, the Main Instance will send instructions on how to execute the action on the Remote Agent.
These instructions take the form of a string containing all the necessary hocon configuration required to execute the Action.
The instructions contain the Action's config along with all its Input and Output DataObjects.
Usually, some of these DataObjects will be linked to connections.
Connections defined in the agents section override connections defined in the connections section when they are executed on the Agent.
This allows the Agent to use some connections that are only accessible in the Agent's environment and not on the Remote Instance.


Supported ways of connecting Main Instance to Remote Agent:

Simple, unsecured Jetty Websocket for development use
Azure Relay service (require sdl-azure and an Azure subscription with an Relay that has a Hybrid Connection)
Only DataObjects that return SparkSubfeeds are supported in a first step
SDLB allows you to  
Some Actions allow only one input and one output, e.g. CopyAction, others can cope with several inputs and outputs, e.g. CustomDataFrameActions. As a best practice implement *n:m* Actions only if you have a good reason, otherwise stick to **1:1**, *1:n* and *n:1* Actions in order to know exact dependencies from metadata.

@startuml
package "Internet" {
cloud {
[Azure Relay Connection]
}
}

package "Cloud" {

[Scheduler] -> [SDL Main Instance] :1
[SDL Main Instance] -> [Azure Relay Connection] :2
[Azure Relay Connection] --> [SDL Main Instance] : 7

database "Cloud Data Source" {
[Cloud Staging] -[#blue,thickness=2]--> [SDL Main Instance] :8
[SDL Main Instance] -[#blue,thickness=2]-> [Cloud Datalake] :9
}

}
package "On Premise" {
[SDL Agent]
database "On-Premise Data Source" {
[On Prem Data]  -[#blue,thickness=2]--> [SDL Agent] :4

[Azure Relay Connection]
[SDL Agent] --> [Azure Relay Connection] :6
[Azure Relay Connection] --> [SDL Agent]  :3

[SDL Agent] -[#blue,thickness=2]> [Cloud Staging] : 5
}  
}

legend right
|Color| Type |
|<#blue>| Data Flow|
|<#black>| Control Flow|
endlegend
@enduml

## Transformations
These can be custom transformers in SQL, Scala/Spark, or Python, OR predefined transformations like Copy, Historization and Deduplication, see [Transformations](transformations).




This functionality is similar to [Expectations](dataQuality#Expectations), but the *metricsFailCondition* is defined on an Action and instead of a DataObject. And it can access all metrics produced by an Action, not the custom metric defined by the Expectation.

