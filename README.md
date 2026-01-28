# Website

This website is built using [Docusaurus 3](https://docusaurus.io/), a modern static website generator.

### Installation
Because the dependency `@smart-data-lake/sdlb-schema-viewer` is hosted on the Github npm registry (https://npm.pkg.github.com/) one needs to [authenticate against Github](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry#authenticating-to-github-packages) to pull the package.
The `.npmrc` of the project sets the token to the environment variable `TOKEN` for the github action.
To install the project locally, set the environment variable `export TOKEN=...` once or in your shell config (e.g. `.zshrc.`, `.bashrc`).
To install the project locally, define the environment variable by running `export TOKEN=...` or add that line to your shell config (e.g., `.zshrc`, `.bashrc`) to make it persistent.

How to generate the token:
- Settings -> Developer settings -> Personal access token
    - Tokens (classic): create on with the permission `read:packages`

And then the following command can be run.

```
$ yarn
```

### Local Development

```
$ yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
$ yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

Using SSH:

```
$ USE_SSH=true yarn deploy
```

Not using SSH:

```
$ GIT_USER=<Your GitHub username> yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
