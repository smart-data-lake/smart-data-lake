import React from 'react';
import Layout from '@theme/Layout';

function JsonSchemaViewer() {
  const ref = React.useRef();
  const [height, setHeight] = React.useState("0px");
  const onLoad = () => {
    setHeight(ref.current.contentWindow.document.body.scrollHeight + "px");
  };
  return (
    <Layout title="JsonSchemaViewer">
      <iframe
        ref={ref}
        onLoad={onLoad}
        id="JsonSchemaViewer"
        src="./json-schema-viewer/index.html"
        width="100%"
        height={height}
        scrolling="no"
        frameBorder="0"
        style={{
          //maxWidth: 640,
          width: "100%",
          overflow: "auto",
        }}
      ></iframe>
    </Layout>
  );
}

export default JsonSchemaViewer;