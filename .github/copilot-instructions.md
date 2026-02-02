# Copilot Instructions for Smart Data Lake Builder

## Project Overview

**Smart Data Lake Builder** is a data lake automation framework built on Apache Spark and Hadoop. It provides a declarative, configuration-driven approach to creating data pipelines for loading and transforming data.

- **Language**: Scala (2.12.18 default, 2.13.12 supported)
- **Build System**: Maven (multi-module project)
- **Main Tech Stack**: Apache Spark 3.5.5, Hadoop 3.3.6, Hive 2.3.9
- **License**: GPLv3
- **Version**: 2.8.2-SNAPSHOT

## Repository Structure

### Main Modules

The repository is organized as a Maven multi-module project:

- **sdl-core**: Core framework (Actions, DataObjects, Workflows) - the main module
- **sdl-kafka**: Kafka integration
- **sdl-deltalake**: Delta Lake connector
- **sdl-iceberg**: Apache Iceberg connector
- **sdl-snowflake**: Snowflake integration
- **sdl-azure**: Azure integration
- **sdl-splunk**: Splunk connector
- **sdl-jms**: JMS connector
- **sdl-lang**: Lab/experimental features
- **sdl-debezium**: Debezium CDC (Change Data Capture) integration

### Key Files

- `pom.xml`: Parent Maven POM with shared configuration
- `CONTRIBUTING.md`: Contribution guidelines (READ THIS FIRST)
- `RELEASE.md`: Release process and versioning
- `README.md`: Project overview and getting started
- `.github/workflows/`: CI/CD pipelines

## Build System

### Maven Basics

```bash
# Build with Scala 2.12 (default)
mvn -P scala-2.12 clean install

# Run tests
mvn -P scala-2.12 -B clean test

# Build excluding Debezium shaded connectors (faster for development)
mvn -P scala-2.12 -B clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'

# Build with Scala 2.13
mvn -P scala-2.13 clean install
```

### Important Maven Profiles

- `scala-2.12` / `scala-2.13`: Scala version selection (2.12 is default)
- `fat-jar`: Build executable fat-jar with all dependencies except Spark/Hadoop
- `fat-jar-with-spark`: Build fat-jar including Spark
- `copy-libs`: Copy runtime dependencies to target/lib
- `test-sonar`: Enable code coverage with Scoverage
- `release-sonatype`: Maven Central release profile

### Module Exclusions for Faster Builds

The Debezium shaded connector modules take a long time to build. For most development work, exclude them:
```bash
-pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'
```

## Testing

### Testing Framework

- **ScalaTest 3.2.19** (primary framework)
- **ScalaTest Maven Plugin 2.0.2** for execution
- JUnit is disabled in favor of ScalaTest

### Test Conventions

- **Test file naming**:
  - Unit tests: `*Test.scala` suffix
  - Integration tests: `*IT.scala` suffix (excluded by default due to rate limiting)
- **Test base classes**:
  - `AnyFunSuite`: Most common test suite
  - `DataObjectTestSuite`: For DataObject tests
  - Mix in `BeforeAndAfter` for setup/teardown
- **Common test utilities**: `io.smartdatalake.testutils.TestUtil`
- **SparkSession**: Use `TestUtil.session` for implicit SparkSession

### Running Tests

```bash
# Run all tests for Scala 2.12
mvn -P scala-2.12 -B clean test

# Run tests for a specific module
mvn -P scala-2.12 -B clean test -pl sdl-core

# Run a specific test class
mvn -P scala-2.12 -B test -Dtest=HousekeepingModeTest -pl sdl-core
```

### Test Resources

- Test configurations: `src/test/resources/`
- Test data files: `src/test/resources/`

## Code Style and Conventions

### Scala Coding Guidelines

Follow the [Scala Style Guide](https://docs.scala-lang.org/style/) with these project-specific conventions:

1. **License Headers**: Every source file MUST include the GPLv3 license header (see existing files for template)
2. **Scaladoc**: Not mandatory for every method, but complex methods must have documentation
3. **Type Annotations**: Only needed for non-obvious types in private fields or local variables
4. **DataFrame/Dataset Naming**: Prefix with `df` or `ds` followed by CamelCase (e.g., `dfCustomers`, `dsOrders`)
5. **Spark Columns**: Use shorthand `$"columnName"` where possible
6. **Logging**: Get logger by mixing in `SmartDataLakeLogger` trait
7. **Configuration Format**: Use HOCON format for all config files

### Code Formatting

- **No scalafmt**: The project does not currently enforce scalafmt in the build pipeline (`scalafmt.skip=true`)
- **IntelliJ Code Style**: Code style is configured in `.idea/codeStyles/` for IntelliJ users
- Use the existing code style in the module you're working on as reference

## Common Development Tasks

### Adding a New DataObject

1. Create class in `sdl-core/src/main/scala/io/smartdatalake/workflow/dataobject/`
2. Extend appropriate base class (e.g., `DataObject`, `CanCreateDataFrame`)
3. Add corresponding test in `sdl-core/src/test/scala/io/smartdatalake/workflow/dataobject/`
4. Follow naming convention: `*DataObject.scala` for implementation, `*DataObjectTest.scala` for test

### Adding a New Action

1. Create class in `sdl-core/src/main/scala/io/smartdatalake/workflow/action/`
2. Extend `Action` or appropriate subclass
3. Add test in `sdl-core/src/test/scala/io/smartdatalake/workflow/action/`
4. Update configuration schema if needed

### Adding a New Module/Connector

1. Create new module directory following pattern `sdl-<connector-name>`
2. Add module to parent `pom.xml` `<modules>` section
3. Create module `pom.xml` inheriting from `sdl-parent`
4. Follow the structure of existing modules (e.g., `sdl-kafka`, `sdl-snowflake`)

## Configuration

### HOCON Format

All configuration files use HOCON (Human-Optimized Config Object Notation):
- Main configuration file: `application.conf`
- Supports includes, substitutions, and comments
- See [HOCON spec](https://github.com/lightbend/config/blob/master/HOCON.md)

### Configuration Structure

Typical configuration includes:
- **connections**: Database, file system, API connections
- **dataObjects**: Input/output data locations and formats
- **actions**: Data processing steps

## CI/CD Workflows

### GitHub Actions Workflows

1. **snapshot_build_2.12.yml**: Builds and tests Scala 2.12 on every push/PR to `develop-spark*` branches
2. **snapshot_build_2.13.yml**: Builds and tests Scala 2.13 with SonarCloud analysis
3. **release.yml**: Release builds from `master-spark*` branches
4. **release_debezium_connector_shaded.yml**: Separate release for Debezium shaded connectors

### CI Build Details

- **Java Version**: JDK 17 (Temurin distribution)
- **Build Command**: `mvn -P scala-2.12 -B clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'`
- **Caching**: Maven repository cached with `~/.m2/repository`
- **SonarCloud**: Only runs for Scala 2.13 builds

### Working with CI

- PRs automatically trigger builds for both Scala 2.12 and 2.13
- Snapshot deployments only happen on `develop-spark*` branches
- Feature/bugfix branches can manually trigger snapshot builds with issue number in version

## Contribution Workflow

### Git Workflow

1. **Branches**:
   - `develop-spark*`: Main development branch (e.g., `develop-spark3`)
   - `master-spark*`: Release branch (e.g., `master-spark3`)
   - `feature/<issueNb>-description`: Feature branches
   - `bugfix/<issueNb>-description`: Bug fix branches
   - `hotfix/<issueNb>`: Hotfix branches from tagged releases

2. **Commit Messages** (see CONTRIBUTING.md):
   - Subject line: Max 50 characters, capitalized, imperative mood, no period
   - Separate subject from body with blank line
   - Body: Wrap at 72 characters
   - Reference issues with autolinked references (e.g., `#123`, `fixes #456`)

### Pull Request Process

1. Fork the repository
2. Create a feature/bugfix branch
3. Make changes following coding conventions
4. Add tests for new functionality
5. Update documentation if needed
6. Create PR with clear title and description (see `.github/pull_request_template.md`)
7. Link related issues in PR description
8. Wait for CI builds to pass
9. Address review feedback

## Common Issues and Workarounds

### Issue 1: Long Build Times

**Problem**: Full build including Debezium shaded connectors takes a long time.

**Workaround**: Exclude the shaded connector modules for development:
```bash
mvn -P scala-2.12 clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'
```

### Issue 2: Integration Tests Failing

**Problem**: Integration tests (`*IT.scala`) may fail due to rate limiting or external service unavailability.

**Workaround**: Integration tests are excluded by default in Maven configuration. Only run them when specifically testing integration functionality.

### Issue 3: Scala Version Compatibility

**Problem**: Need to test against multiple Scala versions (2.12 and 2.13).

**Workaround**: The project supports both via Maven profiles. CI automatically tests both. For local development, use:
```bash
# Scala 2.12 (default)
mvn -P scala-2.12 clean test

# Scala 2.13
mvn -P scala-2.13 clean test
```

### Issue 4: Memory Issues During Build

**Problem**: Maven may run out of memory during compilation or testing.

**Workaround**: Set MAVEN_OPTS environment variable:
```bash
export MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=512m"
```

## Dependencies and Version Management

### Key Dependencies

- Apache Spark: 3.5.5 (default), 3.3.1 (compatibility profile)
- Apache Hadoop: 3.3.6
- Hive: 2.3.9
- ScalaTest: 3.2.19
- Typesafe Config (for HOCON): Managed by Spark
- Databricks libraries: Various connectors

### Updating Versions

Use Maven versions plugin:
```bash
# Display available updates
mvn versions:display-dependency-updates

# Update version in all POMs
mvn versions:set -DnewVersion=x.y.z
```

### Dependency Conflicts

The project uses dependency management in parent POM to resolve conflicts. Check `<dependencyManagement>` section in root `pom.xml` for managed versions.

## IDE Setup

### IntelliJ IDEA

The project includes IntelliJ configuration in `.idea/`:
- Code style: `.idea/codeStyles/`
- Copyright profiles: `.idea/copyright/`
- Run configurations: `.idea/runConfigurations/`
- Scala settings: `.idea/scala_settings.xml`

### Import as Maven Project

1. Open IntelliJ IDEA
2. File → Open → Select root `pom.xml`
3. Import as Maven project
4. Select JDK 17
5. IntelliJ will automatically configure Scala based on Maven settings

## Deployment and Artifacts

### Main Entry Point

The main application entry point is `io.smartdatalake.app.SparkSmartDataLakeBuilder` (defined in Maven shade plugin configuration).

### Artifact Naming

Artifacts are suffixed with Scala version:
- Example: `sdl-core_2.12-2.8.2-SNAPSHOT.jar`
- Pattern: `{artifactId}_${scala.minor.version}-${version}.jar`

### Publishing

- **Snapshot builds**: Automatically published to Sonatype snapshots repository from `develop-spark*` branches
- **Release builds**: Published to Maven Central from `master-spark*` branches
- Requires GPG signing and Sonatype credentials (configured in CI)

## Documentation

### External Documentation

Main documentation is hosted at [smartdatalake.ch/docs](https://smartdatalake.ch/docs):
- Getting Started
- Reference Documentation
- Architecture Guide
- Troubleshooting
- Glossary

### Local Documentation Files

- `README.md`: Project overview
- `CONTRIBUTING.md`: Contribution guidelines
- `GOVERNANCE.md`: Project governance
- `RELEASE.md`: Release process
- `CODE_OF_CONDUCT.md`: Community standards
- `docs/FAQ.md`: Frequently asked questions
- `docs/PublicCloud.md`: Cloud deployment guide

## Quick Reference Commands

```bash
# Build and test (fast - excludes Debezium shaded)
mvn -P scala-2.12 -B clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'

# Build specific module
mvn -P scala-2.12 clean install -pl sdl-core

# Run specific test
mvn -P scala-2.12 test -Dtest=MyTest -pl sdl-core

# Build fat jar
mvn -P scala-2.12,fat-jar clean package

# Check for dependency updates
mvn versions:display-dependency-updates

# Update all version references
mvn versions:set -DnewVersion=x.y.z

# Clean all build artifacts
mvn clean
find . -name target -type d -exec rm -rf {} +
```

## Additional Resources

- Project Homepage: [smartdatalake.ch](https://smartdatalake.ch)
- GitHub Repository: [smart-data-lake/smart-data-lake](https://github.com/smart-data-lake/smart-data-lake)
- Issue Tracker: [GitHub Issues](https://github.com/smart-data-lake/smart-data-lake/issues)
- Apache Spark Documentation: [spark.apache.org](https://spark.apache.org/)
- Scala Documentation: [docs.scala-lang.org](https://docs.scala-lang.org/)
- HOCON Specification: [lightbend/config](https://github.com/lightbend/config/blob/master/HOCON.md)

## Contact and Support

- Email: smartdatalake@elca.ch
- For issues and bugs: Use GitHub Issues
- For questions: Check FAQ.md or create a discussion on GitHub

---

**Last Updated**: 2026-02-02
**Version**: For Smart Data Lake Builder 2.8.2-SNAPSHOT
