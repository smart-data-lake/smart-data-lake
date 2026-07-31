import { useEffect } from 'react';
import { useColorScheme } from '@mui/joy';
import { useColorMode } from '@docusaurus/theme-common';

/**
 * Component for applying the docusaurus color mode to the schema viewer. It needs to be placed inside a CssVarsProvider.
 */
export default function UseColorMode() {
  const { mode, setMode } = useColorScheme(); // schema viewer color mode
  const { colorMode } = useColorMode(); // docusaurus color mode

  useEffect(() => {
    if (mode !== colorMode) {
      setMode(colorMode);
    }
  }, [colorMode]);

  return null;
}