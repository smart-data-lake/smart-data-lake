import React from 'react';
import { SchemaViewer, defaultTheme } from 'sdlb-schema-viewer';
import { CssVarsProvider } from "@mui/joy";
import UseColorMode from "./UseColorMode";
import useBaseUrl from '@docusaurus/useBaseUrl';

export default function SchemaViewerComponent() {
    // UseColorMode needs to be placed inside a CssVarsProvider.
    // When using a CssVarsProvider, the sdlb-schema-viewer defaultTheme has to be specified as the theme,
    // otherwise it is overridden by the @mui/joy default theme.
    return (
        <CssVarsProvider theme={defaultTheme}>
            <UseColorMode/>
            <SchemaViewer loadSchema={loadSchema} loadSchemaNames={loadSchemaNames}/>
        </CssVarsProvider>
    );
}

const schemasUrl = useBaseUrl('/json-schema-viewer/schemas/');

function loadSchemaNames() {
    return fetch(schemasUrl + 'index.json').then(res => res.json());
}

function loadSchema(schemaName) {
    return fetch(schemasUrl + schemaName).then(res => res.json());
}
