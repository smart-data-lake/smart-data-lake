# Release process

## Versioning

We follow [Semantic Versioning Specification 2.0.0](https://semver.org/spec/v2.0.0.html).

In short, given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible API changes,
2. MINOR version when you add functionality in a backwards compatible manner, and
3. PATCH version when you make backwards compatible bug fixes.

## Build types

- **Release builds** are published from master-spark* branches. Release builds are created once and will not be updated.
They are built by creating and merging a pull request from develop-spark* branch to master-spark* branch.

- **Snapshot builds** are published from develop-spark* branches with "-SNAPSHOT" appended to the semantic version.
Snapshot builds are updated until the Release version is published. They are built automatically with every push on develop-spark* branches. 

- **Snapshot builds from bugfix and feature branches** can be created manually.
The issue number is extract from the branch name as additional attribute in the created version, e.g. \<mvnVersion>-\<issueNb>-SNAPSHOT.
To create a Snapshot build from a bugfix or feature branch trigger the "Snapshot Build" Action manually on Github and choose the corresponding branch to build.

## Creating a new Release

To create a new Release from the current state in develop-spark* branch
1. Create a Pull Request from develop-spark* branch to master-spark* branch
2. Review and merge Pull Request 
3. Wait for the build & deploy to succeed. It will
   - Build and deploy the artifacts to oss.sonatype.org
   - Create a tag for the version on master-spark3 branch
   - Increase Snapshot version on develop-spark3 branch
   - Add the new sdl-schema.json file to documentation branch
4. Login to oss.sonatype.org, select "Staging Repositories" and release artifacts to maven central
5. Select created version tag in Github and create Release notes

## Creating a Hotfix Release

To fix problems on previous versions and create a new Hotfix Release
1. Create a new branch named "hotfix/\<issueNb>" from the version tag that you need a fix for
2. Implement & test the fix you need
3. Create a second branch including your fix and create a PR to develop-spark3, so the fix will be included in future versions
4. Update version in all pom.xml files to the Hotfix Version to be created using `mvn versions:set -DnewVersion=x.y.z-SNAPSHOT`
5. Trigger the "Release Build" Action manually on Github and choose the corresponding hotfix branch to build. It will
    - Build and deploy the artifacts to oss.sonatype.org
    - Create a tag for the version on master-spark3 branch
4. Login to oss.sonatype.org, select "Staging Repositories" and release artifacts to maven central
5. Select created version tag in Github and create Release notes