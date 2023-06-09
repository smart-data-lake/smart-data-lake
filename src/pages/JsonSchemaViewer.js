import React from 'react';
import Layout from '@theme/Layout';
import BrowserOnly from '@docusaurus/BrowserOnly';

function JsonSchemaViewer() {
    return (
        <Layout title="JsonSchemaViewer" noFooter={true} wrapperClassName="schema-viewer-wrapper">
            <div style={{flex: 1}}>
                {/* prevent any server side rendering by docusaurus because the schema viewer does not support it */}
                <BrowserOnly>
                    {() => {
                        const SchemaViewerComponent = require('../schema-viewer/SchemaViewerComponent').default;
                        return (<SchemaViewerComponent/>)
                    }}
                </BrowserOnly>
            </div>
        </Layout>
    );
}

export default JsonSchemaViewer;