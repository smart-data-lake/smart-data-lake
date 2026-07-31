# Architecture Documentation using Structurizr

[Structurizr](https://structurizr.com/) is a tool to create architecture "diagrams as code".
It can create different diagrams according to C4 / Arc42 standard from a model defined using a simple "Domain Specific Language".

## Contents
- `workspace.dsl` contains the definition of the model and the diagram views, see [DSL](https://docs.structurizr.com/dsl/language) for the syntax
- `workspace.json` contains the layout of the diagrams created using [Structurizr Lite](https://docs.structurizr.com/lite)
- `diagramExports\` contains the final diagrams exported as SVG (or PNG) using [Structurizr Lite](https://docs.structurizr.com/lite)

## Howto adapt Diagrams

Install IntelliJ Plugin "Structurizr DSL Language Support" for syntax highlighting. There is a similar Plugin for VSCode.

Start [Structurizr Lite](https://docs.structurizr.com/lite) in the project root directory using the following podman command:
```podman run -it --rm -p 8080:8080 -v ./docs/structurizr:/usr/local/structurizr structurizr/lite```

Open [localhost:8080](http://localhost:8080)

Edit `workspace.dsl`, view changes by refreshing [localhost:8080](http://localhost:8080), adapt layout and export diagrams as SVG images to subfolder `diagramExports`.



