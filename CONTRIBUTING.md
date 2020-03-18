# Contributing to Smart Data Lake
We value all input to Smart Data Lake. Thank you for your interest in contributing!
Please read the following guidelines so your contributions can be included more easily. 
 
#### Table Of Contents
[Code of Conduct](#code-of-conduct)

[How to contribute](#how-to-contribute)
* [Report issues](#report-issues)
* [Propose Enhancements](#propose-enhancements)
* [Contribute Code](#contribute-code)
* [Review Changes](#review-changes)
 


## Code of Conduct
All contributions are expected to uphold the [Code of Conduct](CODE_OF_CONDUCT.md). 
If you see any unacceptable behavior, please report it to [smartdatalake@elca.ch](mailto:smartdatalake@elca.ch). 

## How to contribute
Please note that all contributions are automatically licensed under this repository's license (GPLv3) according to the "inbound=outbound" principle of the [GitHub Service Terms](https://help.github.com/en/github/site-policy/github-terms-of-service#6-contributions-under-repository-license)

### Report issues
Before you submit a bug report, please check if it has already been submitted under [Issues](https://github.com/smart-data-lake/smart-data-lake/issues).
If it does, add your comments on the existing issue and provide additional information that might be missing. 
All issues have to be entered in the GitHub repository, so if you can't find it, please open a [new one](https://github.com/smart-data-lake/smart-data-lake/issues/new).

Include a clear **title** and **description** and provide as much information as possible. 
Depending on the use case, try to include a minimum example so that the issue can be reproduced.
Include error messages and stacktraces, make sure to use proper markdown so your report is clear.

If you can not provide sample data, e.g. because of privacy considerations, then try to provide a sample data set to reproduce the problem.

### Propose Enhancements
Same as with issues, please check if a similar enhancement request exists and add a comment instead of opening a new request.

If you have an enhancement idea and can [contribute](#contribute-code) code yourself, please do so!
Alternatively, you can open a new enhancement request. 

To understand your proposal, please include a clear **title** and **description** and provide as much detail
about the background and reasoning behind your request. 
It always helps to see which improvements you hope to see with the enhancement.  

### Contribute Code
Create a fork of smart-data-lake and implement your proposed changes. 
Then create a new GitHub Pull Request for the maintainers to review and merge. 
In your pull request, include a description of what you've changed and the reasoning behind it. 

#### Commit messages
Most of the following guidelines are taken from [here](https://chris.beams.io/posts/git-commit/) as they fit very well to how we work.
* Separate a subject (not more than 50 characters) from a more detailed description of your commit with a blank line.
Small changes don't necessarily need a description.
* Capitalize the subject line and don't end them with a period
* Use the imperative mood in the subject line meaning "spoken or written as if giving a command or instruction".
"Refactor CustomerDfTransformer..., Remove method XYZ..."
* Wrap body at 72 characters, limit subject to 50 characters. 
If you use IntelliJ, configure this under Version Control - Commit Dialog - Limit body line 72, Limit subject line 50.
Most other IDEs have similar settings. 
* Commits should usually belong to an issue or an enhancement and should clearly reference them with [autolinked references](https://help.github.com/en/github/writing-on-github/autolinked-references-and-urls).

In your description, give a background on what has changed and how.
Give the reasoning behind the commit, so fellow developers (and your future self) can understand the changes more quickly.

#### Coding conventions
* Follow the Scala [Style Guide](https://docs.scala-lang.org/style/) with the following remarks:
  * Scaladoc comments are not mandatory for every variable and method, but complex methods must have a description of their functionality
  * Type information for private fields or local variables is only needed for non obvious types
* Prefix variables for Spark DataFrames and Datasets with "df" or "ds" respectively, followed by CamelCase names for quick recognition
* Reference Spark Columns with the short hand $"xyz" where possible
* Get a logger by mixin trait SmartDataLakeLogger
* Include the license text in the header of every file as described in the README

### Review Changes
All issues and enhancements are handled through pull requests. 
Anyone is open to contribute to active pull requests by reviewing code and participating in open discussions and questions.
