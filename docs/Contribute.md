# Contributing

Please check the [Contributing Guidelines](../CONTRIBUTING.md) and [Code of Conduct](../CODE_OF_CONDUCT.md)
first. 
You'll find some additional information on how to setup your development environment.

## Formatting License Headers

To add/update license headers to ***all*** project source files, execute:

`mvn license:format`

The license header template is located in `src/license` and values for the variables are taken from the `pom.xml` configuration.

### Setup License Header in IntelliJ

* Open: File - Settings - Editor - Copyright - Copyright Profiles and add (+) a new profile:
    * Name: GPLv3
    * Copyright Text: 
```
Smart Data Lake - Build your data lake the smart way.

Copyright © 2019-${today.year} ELCA Informatique SA (<https://www.elca.ch>)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
```
* Go to Copyright - Formatting and set:
    * Comment Type: Choose "Use block comment" and activate "Prefix each line".
    * Relative Location: Choose "Before other comments"
    * Border: Activate "Add blank line after" and deactivate "Separator before" and "Separator after"
* Go to Copyright and add (+) a new scope:
    * Scope: Non-Project Files
    * Copyright: GPLv3
