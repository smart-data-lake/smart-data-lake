# Architecture Documentation using Structurizr

[Structurizr](https://structurizr.com/) is a tool to create architecture "diagrams as code".
It can create different diagrams according to C4 / Arc42 standard from a model defined using a simple "Domain Specific Language".

## Contents
- `workspace.dsl` contains the definition of the model and the diagram views, see [DSL](https://docs.structurizr.com/dsl/language) for the syntax
- `diagramExports\` contains the final diagrams exported as PNG

## Howto adapt Diagrams

Install IntelliJ Plugin "Structurizr DSL Language Support" for syntax highlighting. There is a similar Plugin for VSCode.

Start Structurizr in the project root directory using the following podman command
(Structurizr Lite is deprecated, its successor is the `local` mode of the `structurizr/structurizr` image):
```podman run -it --rm -p 8080:8080 -v ./docs/docs/structurizr:/usr/local/structurizr structurizr/structurizr local```

The directory must be writable by the container user, e.g. `chmod a+rwX docs/docs/structurizr`.

Open [localhost:8080](http://localhost:8080)

Edit `workspace.dsl` and view changes by refreshing [localhost:8080](http://localhost:8080).
The views use `autoLayout`, so new elements need no manual layout.
Export the diagrams and their keys as PNG images to subfolder `diagramExports`, named `container-001.png`,
`container-001-legend.png`, `component-001.png` and `component-001-legend.png`.
