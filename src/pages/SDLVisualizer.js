import React from 'react';
import Layout from '@theme/Layout';
import useIsBrowser from '@docusaurus/useIsBrowser';

function SDLVisualizer() {
    //same warpper as schemaviewer
    const SDLVisualizerInBrowser = useIsBrowser ? require('../sdl-visualizer/SDLVisualizerComponent').default : (<div />);

    return (
        <Layout title="JsonSchemaViewer" noFooter={true} wrapperClassName="schema-viewer-wrapper">
            <div style={{flex: 1}}>
                <SDLVisualizerInBrowser  />
            </div>
        </Layout>
    );
}

export default SDLVisualizer;