import React from 'react';
import Layout from '@theme/Layout';
import useIsBrowser from '@docusaurus/useIsBrowser';

function VisualizerDemo() {
    //same warpper as schemaviewer
    const SDLVisualizerInBrowser = useIsBrowser ? require('../sdl-visualizer/SdlVisualizerComponent').default : (<div />);

    return (
        <Layout title="sdl-viz" noFooter={true} wrapperClassName="schema-viewer-wrapper">
            <div style={{flex: 1}}>
{/*                 <SDLVisualizerInBrowser  /> */}
            <iframe src="./sdl-viz/" width="100%" height="1000px" > </iframe>
            </div>
        </Layout>
    );
}

export default VisualizerDemo;