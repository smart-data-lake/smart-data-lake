import React from 'react';
import Layout from '@theme/Layout';
import { SchemaViewer } from 'sdlb-schema-viewer';

function JsonSchemaViewer() {
  return (
    <Layout title="JsonSchemaViewer">
      <div style={{height: '90vh'}}>
        {/* This does not show the local schemas but only the deployed ones.
            The schema viewer must be adapted to be able to show local schemas. */}
        <SchemaViewer schemasUrl="https://smartdatalake.ch/json-schema-viewer/schemas/"/>
      </div>
    </Layout>
  );
}

export default JsonSchemaViewer;