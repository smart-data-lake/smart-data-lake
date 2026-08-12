import React from 'react';
import Layout from '@theme/Layout';
import useIsBrowser from '@docusaurus/useIsBrowser';

function JsonSchemaViewer() {
    // only load schema viewer in browser because the schema viewer does not support server side rendering
    const SchemaViewerInBrowser = useIsBrowser ? require('../schema-viewer/SchemaViewerComponent').default : (<div />);

    return (
        <Layout title="JsonSchemaViewer" noFooter={true} wrapperClassName="schema-viewer-wrapper">
            <SchemaViewerInBrowser />
        </Layout>
    );
}

export default JsonSchemaViewer;