# Minibase Project Setup

| File | Author | Date | Version|
|------|--------|------|--------|
| README.md | Michael Grossniklaus <michael.grossniklaus@uni.kn> | April 26, 2014 | 1.0 |

## Introduction

This file provides instructions on how to set up the Minibase project for development on
your computer. Throughout these instructions, we assume that you are using the Eclipse
integrated development environment for Java. Although using Eclipse is not a requirement 
in order to contribute to the Minibase source code, it is strongly recommended! You are
responsible for any problems or complications that you incur or cause for others, if you
decide to use an IDE other than Eclipse.

## Prerequisites

The following software should be installed on your computer before you begin to work
through the following instructions.

1. [Java Platform (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html), 
   version 8 or higher
2. [Apache Maven](http://maven.apache.org/download.cgi), version 3.2.0 or higher
3. [Eclipse IDE for Java Developers](http://www.eclipse.org/downloads/), version 4.4.1 or 
   higher
4. [Checkstyle Eclipse plug-in](http://eclipse-cs.sourceforge.net), version 5.9 or higher

## Configuring Eclipse

Before the Minibase project can be imported into Eclipse, the required project files
need to be generated using Maven. In the directory where you checked out the Minibase
source code, execute the following command.

```sh
mvn clean eclipse:clean eclipse:eclipse
```

Remember to re-execute this command, whenever you or somebody else updates the `pom.xml`
file in the main directory of the Minibase source code.
   
After the successful completion of the above Maven command, open Eclipse and import the 
Minibase project with the wizard under `File` → `Import...`, choosing the option 
"Existing Projects into Workspace" in category "General".

Once the project has been created, open the project properties and open the "Java Code
Style" category. For each of the four sub-categories, "Clean Up", "Code Templates", 
"Formatter", and "Organize Imports", import the corresponding configuration file from
the `config/eclipse/` directory in your local copy of the Minibase source code.

Finally, check that Checkstyle is activated for the Minibase project. Creating the
Eclipse project with Maven will have already configured Checkstyle correctly. To make 
sure it is activated, select `Checkstyle` → `Activate Checkstyle` from the project
context menu.

*Congratulations, you are good to go!*
