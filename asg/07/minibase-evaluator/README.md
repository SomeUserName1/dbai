# Minibase for Java

Minibase is a single-user database management system mainly intended for educational use.
Students can use Minibase to study and implement several DBMS components, without support for concurrency control and recovery.
The source code of Java version that is under development at the University of Konstanz is based on Chris Mayfield's Minibase for Java, which extended and redesigned the initial Java port of the original C/C++ source code.
This first version of Minibase was developed by Raghu Ramakrishnan for the practical exercises that accompany the book Database Management Systems.
Minibase's history traces back to Minirel course project developed by David DeWitt at the University of Wisconsin-Madison.

## Prerequisites

To work with the Minibase for Java source code, the following software must be present on the development machine.

 * [Java Platform (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
 * [git](http://git-scm.com/)
 * [Apache Maven](http://maven.apache.org/)

## Obtaining the Source Code

The source code is available from the GitHub repository of the Databases group at the University of Konstanz. Contact [Michael Grossniklaus](mailto:michael.grossniklaus@uni.kn) to check if you are eligible to get access to this repository. Once your access has been cleared you can clone the source code from this URL: [https://github.com/DBIS-UniKN/minibase.git](https://github.com/DBIS-UniKN/minibase.git).

## Hall of Fame

- **Johann Bornholdt**
- **Wai-Lok Cheung**
- **Michael Delz**
- **Michael Grossniklaus**
- **Marcel Hanser**
- **Jürgen Hölsch**
- **Manuel Hotz**
- **Manfred Schäfer**
- **Nadja Sadowski**
- **Raffael Wagner**
- **Leonard Wörteler**

## Funding

The Ministry of Science, Research and Art (Ministerium für Wissenschaft, Forschung und Kunst) of Baden-Württemberg is funding extensions to Minibase that help to improve the software for its use in education.
In particular, this grant is used to extend the documentation of Minibase, to create new programming assignments, and to prepare better solutions to the assignments.

## Contributing

If you want to contribute to the project contact [Michael Grossniklaus](mailto:michael.grossniklaus@uni.kn) to see, which access  to the repository you can get.

So, contributions are centered around Pull Requests. They provide documented feature additions and support discussion about changes. 
Also, this way our Jenkins-CI server picks your changes up, does all the tests and reports if the Pull Request is o.k.

### Write Access

If you have direct write access to the repository, the workflow is basically as follows:

1. make a new branch, call it something meaningful or gh-ISSUENUMBER, where ISSUENUMBER is the number of the issue you want to adress
2. commit the changes in semantically meaningful commits (group logical changes toghether)
3. make a Pull Request on GitHub

Of course, when we have very small changes, we could argue about directly committing to master, but this way everything is clear, documented and we can be confident that the tests still pass.

### Read Access

If you only get read access, you can fork the repository, push your branch to your fork and then create a Pull Request from `your-fork/branch` to `this-repository/master`.
