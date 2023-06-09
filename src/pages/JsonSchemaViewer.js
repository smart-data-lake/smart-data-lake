import React from 'react';
import Layout from '@theme/Layout';
import { SchemaViewer, defaultTheme } from 'sdlb-schema-viewer';
import { CssVarsProvider } from "@mui/joy";
import UseColorMode from "./UseColorMode";

function JsonSchemaViewer() {
  return (
    <Layout title="JsonSchemaViewer" noFooter={true} wrapperClassName="schema-viewer-wrapper">
      <div style={{flex: 1}}>
        {/* UseColorMode needs to be placed inside a CssVarsProvider.
            When using a CssVarsProvider, the sdlb-schema-viewer defaultTheme has to be specified as the theme,
            otherwise it is overridden by the @mui/joy default theme. */}
          <CssVarsProvider theme={defaultTheme}>
              <UseColorMode />
              <SchemaViewer loadSchema={loadSchema} loadSchemaNames={loadSchemaNames} />
          </CssVarsProvider>
      </div>
    </Layout>
  );
}

const schemasUrl = window.location.protocol + '//' + window.location.host + '/json-schema-viewer/schemas/';

function loadSchemaNames() {
   return fetch(schemasUrl + 'index.json').then(res => res.json());
}

function loadSchema(schemaName) {
    return fetch(schemasUrl + schemaName).then(res => res.json());
}

export default JsonSchemaViewer;