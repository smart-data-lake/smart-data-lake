# Lighttpd

Install small Webserver "lighttpd"
`sudo apt install lighttpd`

Adapt lighttpd.conf to your needs. Default setup is to listen on port 5000 for http on all interfaces.
For production use an https proxy and authentication should be put in front, e.g. oauth2-proxy, and server.bind set to 127.0.0.1.
The default configuration serves config, envConfig and description directory from the parent folder, and the rest from this directory. This allows to have a subdirectory with sdl-visualization release files, serving the SDLB configuration of your project. 

Start serving by executing the following command in the directory of this README file:
`lighttpd -D -f lighttpd.conf`
 Note: parameter `-D` is to start lighttpd in non-deamonized mode.