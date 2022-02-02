import React, {useEffect} from 'react';
import Layout from '@theme/Layout';

function JsonSchemaViewer() {
  
  const onLoad = () => {
    const wrapper = document.getElementsByClassName("main-wrapper")[0]
    wrapper.style.display = 'flex'
  };
  
  return (
    <Layout title="JsonSchemaViewer" >
        <iframe
          onLoad={onLoad}
          id="JsonSchemaViewer"
          src="/json-schema-viewer/index.html"
          width="100%"
          scrolling="no"
          frameBorder="0"
          style={{
            flex: '1',
          }}
        ></iframe>
    </Layout>    
  );
}

export default JsonSchemaViewer;