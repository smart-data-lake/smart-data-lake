import React from 'react';
import Layout from '@theme/Layout';
import BrowserOnly from '@docusaurus/BrowserOnly';

function JsonSchemaViewer() {
    // only load schema viewer in browser because the schema viewer does not support server side rendering
    return (
        <Layout title="JsonSchemaViewer" noFooter={true} wrapperClassName="schema-viewer-wrapper">
            <BrowserOnly fallback={<div />}>
                {() => {
                    const SchemaViewerComponent = require('../schema-viewer/SchemaViewerComponent').default;
                    return <SchemaViewerComponent />;
                }}
            </BrowserOnly>
        </Layout>
    );
}

export default JsonSchemaViewer;
