---
id: dag
title: DAG
---

:::warning
This page is under review and currently not visible in the menu.
:::

The Directed Acyclic Graph (DAG) describes the data pipeline and the dependencies between actions. 
It is build automatically within SDLB by analyzing the actions, related dataObjects and its dependencies. 
SDLB uses this DAG to optimize execution of the pipeline as it knows in which order the pipeline needs to be executed.
Because it's acyclic, it cannot have loops, so it's impossible to define an action that depends on an output of subsequent actions.

This has the advantage, that we do not not have to explicitly define the order of actions and not the dependencies between actions. 
Especially for complex structures the configuration is more easy to maintain. 

Further, independent actions are identified, which SDLB can execute in parallel. 

If you don't get the results you expect, it's good to check if the DAG looks correct.

An example could look like:

```
                                        ┌───────────────┐
                                        │     start     │
                                        └─┬─┬───┬─┬┬┬─┬─┘
                                          │ │   │ │││ │
              ┌───────────────────────────┘ │   │ │││ └──────────────────────────────────┐
              │                     ┌───────┘   │ ││└────────────────────┐               │
              │                     │           │ ││                     │               │
              v                     v           │ ││                     │               │
       ┌─────────────┐       ┌─────────────┐    │ ││                     │               │
       │download-tab3│       │download-tab2│    │ ││                     │               │
       └────┬────┬───┘       └────┬────┬───┘    │ ││                     │               │
            │    │                │    │ ┌──────┘ ││                     │               │
 ┌──────────┘    │                │    │ │        ││                     │               │
 │     ┌─────────┘                │    │ │        ││                     │               │
 │     │   ┌──────────────────────┘    │ │        ││                     │               │
 │     │   │    ┌──────────────────────┘ │        ││                     │               │
 │     │   │    │        ┌───────────────┼────────┼┘                     │               │
 │     │   │    │        │               │        └──────┐               │               │
 │     │   │    │        │               │               │               │               │
 │     v   v    │        v               v               v               v               v
 │ ┌──────────┐ │ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
 │ │get_tab323│ │ │download-tab1│ │download-tab6│ │download-tab5│ │download-tab4│ │download-tab7│
 │ └─────┬────┘ │ └──────┬──────┘ └──────┬──────┘ └┬────────────┘ └──────┬──────┘ └──────┬──────┘
 │       │      │        │               │         │                     │               │
 │       │      │        │               │         │ ┌───────────────────┘               │
 │       │      │        └───────────────┼─────┐   │ │ ┌─────────────────────────────────┘
 │       │      └────────────────────────┼───┐ │   │ │ │
 │       └───────────────────────────────┼─┐ │ │   │ │ │
 └───────────────────────────────────────┼┐│ │ │   │ │ │
                                         │││ │ │   │ │ │
                                         vvv v v   v v v
                                       ┌─────────────────┐
                                       │    get_data     │
                                       └─────────────────┘
```
