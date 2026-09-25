import React from 'react';
import { SchemaViewer, defaultTheme } from '@smart-data-lake/sdlb-schema-viewer';
import { CssVarsProvider } from "@mui/joy";
import UseColorMode from "./UseColorMode";
import useBaseUrl from '@docusaurus/useBaseUrl';

export default function SchemaViewerComponent() {
    const schemasUrl = useBaseUrl('/json-schema-viewer/schemas/');
    const loadSchemaNames = () => fetch(schemasUrl + 'index.json').then(res => res.json());
    const loadSchema = (schemaName) => fetch(schemasUrl + schemaName).then(res => res.json());

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
