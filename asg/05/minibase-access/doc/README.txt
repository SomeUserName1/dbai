This version of Minibase for Java is a work in progress.  For more details,
please see HISTORY.txt and TODO.txt.  The following directories are included
with this release:

  grading  : Notes for sample grading criteria
  handouts : Sample project handouts (somewhat outdated)
  javadocs : Complete documentation for the skeleton code
  skeleton : Optional starting code to release to students
  solution : Working solution code (please protect this)

Each java package corresponds to a major component of the database system.
When completed, example course projects will include:

  1. DiskMgr and BufMgr
  2. HeapFile and HFPage
  3. BTreeIndex or HashIndex
  4. ExtSort and DupElim
  5. SortMerge or HashJoin
  6. Catalog and QueryCheck
  7. Query Optimization

Since each package is mostly self-contained, the instructor is free to pick
and choose which projects are most suitable for his or her course.  In an
effort to keep the solution code from floating around the Internet, please
adhere to the following guidelines when designing projects:

1. Release only the minimal amount of files (both source and documentation)
   necessary to build and complete the project.

2. Do not make ANY source code accessible via the web, i.e. where Google can
   cache it and thus provide solutions to future students, including those at
   other universities using these same projects.

3. If releasing .jar files of the components that students will not implement,
   please obfuscate them (see solution/lib/README.txt) to prevent simple
   decompilation and distribution of the solution code.

I hope these extensions and new features will be useful to others teaching
courses on database systems.  Please feel free to send me any questions,
comments, or enhancements via email at cmayfiel@cs.purdue.edu.

Chris Mayfield
August 2, 2006
